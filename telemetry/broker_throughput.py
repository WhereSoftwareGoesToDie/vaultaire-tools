#!/usr/bin/env python 
'''use burstnetsink to tap a broker's debug stream and output throughput info

e.g.:

    $ burstnetsink -v -d -b tcp://broker:5000 2>&1 | broker_throughput.py
'''

import sys
from time import *
import os
import fcntl

class TimeAware(object):
    '''simple timing aware mixin
    when check_for_tick_changed() is called on_tick_change is passed
    iff 'ticklen' seconds has passed since last time check_for_tick_changed

    The default implementation of on_tick_func is to call every function
    passed to the constructor in tick_handlers
    '''
    def __init__(self, ticklen=1, tick_handlers=[]):
        self.last_tick = time()
        self.ticklen = ticklen
        self.tick_handlers = tick_handlers
    def check_for_tick_changed(self):
        tnow = time()
        if tnow - self.last_tick >= 1:
            self.on_tick_change()
            self.last_tick = tnow
    def on_tick_change(self):
        for f in self.tick_handlers: f()
    def run_forever(self,sleep_time=1.0):
        while True:
            self.check_for_tick_changed()
            sleep(sleep_time)

class TimeHistogram(TimeAware):
    '''implements a rolling histogram'''
    def __init__(self, nbins, seconds_per_bin=1):
        TimeAware.__init__(self, seconds_per_bin)
        self.nbins = nbins
        self._bins = [0 for n in range(nbins)]
        self.current_bin = 0
    def on_tick_change(self):
        self.current_bin = (self.current_bin + 1) % self.nbins
        self._bins[self.current_bin] = 0
    def add(self, n=1):
        '''add 'n' to the current histogram bin
        '''
        self.check_for_tick_changed()
        self._bins[self.current_bin] += n
    def sum(self, k=60):
        '''return the total entries per second over the last k seconds
        '''
        bins_to_check = k/self.ticklen
        return sum(self.bins[-bins_to_check:])
    def mean(self, k=60):
        '''return the mean entries per second over the last k seconds
        '''
        bins_to_check = k/self.ticklen
        return self.sum(k) / float(bins_to_check) if bins_to_check else 0
    @property 
    def bins(self):
        '''get bins in time order, oldest to newest'''
        self.check_for_tick_changed()
        return self._bins[self.current_bin+1:]+self._bins[:self.current_bin+1]

class ThroughputCounter(object):
    def __init__(self):
        self.burst_hist = TimeHistogram(600) 
        self.ack_hist = TimeHistogram(600) 

    def add_bursts(self, n): self.burst_hist.add(n)
    def add_acks(self, n): self.ack_hist.add(n)
   
    def get_outstanding(self,last_n_seconds=[10,60,300,600]):
        total_burst_counts = map(self.burst_hist.sum, last_n_seconds)
        total_ack_counts = map(self.ack_hist.sum, last_n_seconds)
        return [nbursts-nacks for nbursts,nacks in zip(total_burst_counts,total_ack_counts)]
    def get_bursts_per_second(self,over_seconds=[10,60,300,600]):
        return map(self.burst_hist.mean, over_seconds)
    def get_acks_per_second(self,over_seconds=[10,60,300,600]):
        return map(self.ack_hist.mean, over_seconds)

    def process_line(self, line):
        if 'received ' in line: 
            self.add_bursts(1)
        elif 'got ingestd ACK' in line:
            self.add_acks(1)

if __name__ == '__main__':
    watcher = ThroughputCounter()    

    # Make stdin non-blocking
    fd = sys.stdin.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


    def process_stdin():
        while True:
            try:
                l = sys.stdin.readline()
                watcher.process_line(l)
            except IOError:
                return

    def print_throughput():
        avgtimes = (300,60,10)
        bursts_per_second = ["%.1f" % n for n in watcher.get_bursts_per_second(avgtimes)]
        acks_per_second = ["%.1f" % n for n in watcher.get_acks_per_second(avgtimes)]
        outstanding_totals = map(str, (max(0,n) for n in
            watcher.get_outstanding(avgtimes)))

        groups = zip(avgtimes, bursts_per_second, acks_per_second, outstanding_totals)

        for seconds,bps,aps,outstanding in groups:
            print "(last %ss)%s bursts/sec, %s acks/sec, %s backlog" %( seconds,bps,aps,outstanding ),
        print
        sys.stdout.flush()

    printer = TimeAware(1, [process_stdin, print_throughput])
    printer.run_forever(0.1)
