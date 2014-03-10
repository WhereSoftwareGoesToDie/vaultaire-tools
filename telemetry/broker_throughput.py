#!/usr/bin/env python 

'''use burstnetsink to tap a broker's debug stream and output throughput info

e.g.:

    $ burstnetsink -v -p -b tcp://broker:5000 2>&1 | broker_throughput.py
'''

import sys
from time import *
import os
import fcntl

class TimeAware(object):
    '''simple timing aware mixin

    The default implementation of on_tick_change() is to call every function
    passed to the constructor in tick_handlers
    '''
    def __init__(self, ticklen=1, tick_handlers=[]):
        self.last_tick = self.start_time = time()
        self.ticklen = ticklen
        self.tick_handlers = tick_handlers
        self.n_ticks = 0
        self.totalticktime = 0
    def check_for_tick_changed(self):
        '''run on_tick_change once for every ticklen seconds that has passed since last_tick
        '''
        tnow = time()
        while tnow - self.last_tick >= self.ticklen:
            self.n_ticks += 1
            self.totalticktime += self.ticklen
            self.last_tick += self.ticklen
            self.on_tick_change()
    def on_tick_change(self):
        '''handler for a tick change
        the timestamp marking the 'tick' being handled is in self.last_tick
        The current time may however be significantly after self.last_tick if
        check_for_tick_changed is not called more often than self.ticklen
        '''
        for f in self.tick_handlers: f()
    def run_forever(self,sleep_time=None):
        '''run in a loop regularly calling on_tick_change
        '''
        if sleep_time == None: sleep_time = self.ticklen/10.0
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
        if self.totalticktime < k:
            k = self.totalticktime  # Only average over the time we've been running
        bins_to_check = k/self.ticklen
        return self.sum(k) / float(bins_to_check) if bins_to_check else 0
    @property 
    def bins(self):
        '''get bins in time order, oldest to newest'''
        self.check_for_tick_changed()
        return self._bins[self.current_bin+1:]+self._bins[:self.current_bin+1]

class ThroughputCounter(object):
    def __init__(self):
        self.point_hist = TimeHistogram(600) 
        self.burst_hist = TimeHistogram(600) 
        self.acked_burst_hist = TimeHistogram(600) 
        self.latency_hist = TimeHistogram(600) 
        self.ack_hist = TimeHistogram(600) 
        self.outstanding_bursts = {}  # burstid -> start timestamp
        self._reader_state = {}

    def get_outstanding(self,last_n_seconds=[10,60]):
        total_burst_counts = map(self.point_hist.sum, last_n_seconds)
        total_ack_counts = map(self.ack_hist.sum, last_n_seconds)
        return [nbursts-nacks for nbursts,nacks in zip(total_burst_counts,total_ack_counts)]
    def get_points_per_seconds(self,over_seconds=[300,60,10]):
        return map(self.point_hist.mean, over_seconds)
    def get_total_bursts(self,over_seconds=[300,60,10]):
        return map(self.burst_hist.mean, over_seconds)
    def get_acks_per_second(self,over_seconds=[300,60,10]):
        return map(self.ack_hist.mean, over_seconds)
    def get_average_latencies(self,over_seconds=[300,60,10]):
        burst_counts = map(self.acked_burst_hist.sum, over_seconds)
        latency_sums = map(self.latency_hist.sum, over_seconds)
        return [latencysum/float(nbursts) if nbursts > 0 else 0 for latencysum,nbursts in zip(latency_sums,burst_counts)]

    def process_burst(self, data):
        if not all(k in data for k in ('identity','message id','points')): 
            print >> sys.stderr, 'malformed databurst info. ignoring'
            return
        msgtag = data['identity']+data['message id']
        points = int(data['points'])
        timestamp = time()
        self.outstanding_bursts[msgtag] = timestamp,points
        self.burst_hist.add(1)
        self.point_hist.add(points)

    def process_ack(self, data):
        if not all(k in data for k in ('identity','message id')): 
            print >> sys.stderr, 'malformed ack info. ignoring'
            return

        msgtag = data['identity']+data['message id']
        if msgtag not in self.outstanding_bursts:
            print >> sys.stderr, 'got ack we didn\'t see the burst for. ignoring it. This is perfectly fine'
            return

        burst_timestamp,points = self.outstanding_bursts.pop(msgtag)
        latency = time() - burst_timestamp
        self.ack_hist.add(points)
        self.acked_burst_hist.add(1)
        self.latency_hist.add(latency)
        print >> sys.stderr, "ack for",msgtag,"- latency:",latency

    def process_line(self, line):
        '''process a line of burstnetsink trace output

        sample:
            got ingestd ACK
                identity:   0x00e43c9880
                message id: 0xeb9a
            received 5222 bytes
                identity:   0x00e43c9877
                message id: 0xf394
                compressed: 5214 bytes
                uncompressed:       61133 bytes
                points:             405
        '''
        line = line.strip()
        state = self._reader_state
        k,tok,v = line.partition(':')
        if tok == ':':
            state[k] = v.strip()
            if state.get('reading') == 'burst' and k == 'points':
                self.process_burst(state)
                self_reader_state = state = {}
            if state.get('reading') == 'ack' and k == 'message id':
                self.process_ack(state)
                self_reader_state = state = {}
        if 'received ' in line: 
            self._reader_state = {'reading':'burst', 'bytes': int(line.split()[1])}
        elif 'got ingestd ACK' in line:
            self._reader_state = {'reading':'ack'}

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
        bursts_per_second = watcher.get_points_per_seconds(avgtimes)
        acks_per_second = watcher.get_acks_per_second(avgtimes)
        outstanding_totals = map(str, (max(0,n) for n in watcher.get_outstanding(avgtimes)))
        average_latencies = watcher.get_average_latencies(avgtimes)

        groups = zip(avgtimes, bursts_per_second, acks_per_second,
                average_latencies, outstanding_totals) 

        for seconds,pps,aps,latency,backlog in groups:
            #print "last %3ds: %.2f points/sec in - %.2f acked points/sec.- avglatency: %.2f seconds.  backlog: %s points" %( seconds, pps, aps, latency, backlog )
            print "(%ssec) incoming_points_per_second:%.2f acked_point_per_second:%.2f mean_ack_latency:%.2f backlogged_points:%s"  %( seconds, pps, aps, latency, backlog )
        print
        sys.stdout.flush()

    printer = TimeAware(1, [process_stdin, print_throughput])
    printer.run_forever()
