#!/usr/bin/env python 

'''use marquise_telemetry to build throughput info as visible from the client

e.g.:

    $ marquse_telemetry broker | marquise_throughput.py
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
    def __init__(self, input_stream=sys.stdin):
        self.input_stream=input_stream

        self.point_hist = TimeHistogram(600) 
        self.burst_hist = TimeHistogram(600) 
        self.acked_burst_hist = TimeHistogram(600) 
        self.latency_hist = TimeHistogram(600) 
        self.ack_hist = TimeHistogram(600) 
        self.defer_write_points_hist = TimeHistogram(600) 
        self.defer_read_points_hist = TimeHistogram(600) 
        self.timed_out_points_hist = TimeHistogram(600) 

        self.outstanding_points = 0
        self.outstanding_bursts = {}  # burstid -> start timestamp,points
        self._reader_state = {}
        self.using_marquised = set() # Hosts that relay through marquised

    def get_outstanding(self,last_n_seconds=[600,60,1]):
        total_burst_counts = map(self.point_hist.sum, last_n_seconds)
        total_ack_counts = map(self.ack_hist.sum, last_n_seconds)
        return [nbursts-nacks for nbursts,nacks in zip(total_burst_counts,total_ack_counts)]
    def get_total_outstanding_points(self):
        return sum(points for timestamp,points in self.outstanding_bursts.itervalues())
    def get_points_per_seconds(self,over_seconds=[600,60,1]):
        return map(self.point_hist.mean, over_seconds)
    def get_total_bursts(self,over_seconds=[600,60,1]):
        return map(self.burst_hist.mean, over_seconds)
    def get_acks_per_second(self,over_seconds=[600,60,1]):
        return map(self.ack_hist.mean, over_seconds)
    def get_deferred_points_written_per_second(self,over_seconds=[600,60,1]):
        return map(self.defer_write_points_hist.mean, over_seconds)
    def get_timed_out_points_per_second(self,over_seconds=[600,60,1]):
        return map(self.timed_out_points_hist.mean, over_seconds)
    def get_deferred_points_read_per_second(self,over_seconds=[600,60,1]):
        return map(self.defer_read_points_hist.mean, over_seconds)
    def get_average_latencies(self,over_seconds=[600,60,1]):
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
        self.outstanding_points += points
        self.burst_hist.add(1)
        self.point_hist.add(points)
    def _msg_tag_from_data(self, data):
        return (data['identity'].replace('marquised:',''))+data['message id']
    def process_deferred_write(self, data):
        msgtag = self._msg_tag_from_data(data)
        burst_timestamp,points = self.outstanding_bursts.get(msgtag,(None,None))
        if burst_timestamp is not None:
            self.defer_write_points_hist.add(points)
    def process_deferred_read(self, data):
        msgtag = self._msg_tag_from_data(data)
        burst_timestamp,points = self.outstanding_bursts.get(msgtag,(None,None))
        if burst_timestamp is not None:
            self.defer_read_points_hist.add(points)
    def process_send_timeout(self, data):
        msgtag = self._msg_tag_from_data(data)
        burst_timestamp,points = self.outstanding_bursts.get(msgtag,(None,None))
        if burst_timestamp is not None:
            self.timed_out_points_hist.add(points)
    def process_ack(self, data):
        if not all(k in data for k in ('identity','message id')): 
            print >> sys.stderr, 'malformed ack info. ignoring'
            return

        if data['identity'][:10] == 'marquised:':
            # ACK is coming back to marquised from the broker
            host = data['identity'][10:]
            self.using_marquised.add(host)
        else:
            host = data['identity']
            if host in self.using_marquised:
                # If a client is using marquised, that client will
                # recieve an ack back from marquised immediately.
                #
                # We ignore this ack here, and wait for the one
                # received by marquised
                return 

        msgtag = host+data['message id']
        burst_timestamp,points = self.outstanding_bursts.pop(msgtag,(None,None))

        if burst_timestamp == None:
            # Got an ACK we didn't see the burst for. Ignoring it.
            return

        latency = time() - burst_timestamp
        self.ack_hist.add(points)
        self.acked_burst_hist.add(1)
        self.latency_hist.add(latency)
        self.outstanding_points -= points

    def process_line(self, line):
        '''process a line of marquise telemetry

        At the moment, only look at bursts being created by the collate_thread
        and acked by the marquise poller_thread

        sample:
            fishhook.engineroom.anchor.net.au 1395212041732118000 8c087c0b collator_thread created_databurst frames = 1618 compressed_bytes = 16921
        ....
            marquised:astrolabe.syd1.anchor.net.au 1395375377705126042 c87ba112 poller_thread rx_msg_from collate_thread
        ....
            fishhook.engineroom.anchor.net.au 1395212082492520000 8c087c0b poller_thread rx_ack_from broker msg_id = 5553

        CAVEAT: In the above, the marquised 'collate_thread' is actually the 
        collate thread in a different process, received by marquised. We can
        use the knowledge that this is happening to note that astrolabe is
        passing stuff through marquised, and to ignore the ACK that marquised
        sends back to the original client on astrolabe when tracking end-to-end
        latency
        '''
        fields = line.strip().split()

        if len(fields) < 4: return

        # Keep track of hosts using marquised. This is a bit bruteforce, but we need to catch this
        # sort of thing early to not accidentally double-track ACKs
        #
        if fields[0][:10] == 'marquised:':
            self.using_marquised.add(fields[0][10:])

        key = ' '.join(fields[3:6])
        if key == 'collator_thread created_databurst frames':
            identity,message_id,points = fields[0],fields[2],int(fields[7]) 
            self.process_burst({ 'identity': identity, 'message id': message_id, 'points': points })

        # Anything past here is only in the poller thread. Skips a lot of stuff
        if fields[3] != 'poller_thread': return

        if key == 'poller_thread rx_ack_from broker':
            identity,message_id = fields[0],fields[2]
            self.process_ack({ 'identity': identity, 'message id': message_id })
        elif fields[4] == 'defer_to_disk':
            identity,message_id = fields[0],fields[2]
            data = { 'identity': identity, 'message id': message_id }
            self.process_deferred_write({ 'identity': identity, 'message id': message_id })
            if fields[5] == 'timeout_waiting_for_ack':
                self.process_send_timeout({ 'identity': identity, 'message id': message_id })
        elif fields[4] == 'read_from_disk':
            identity,message_id = fields[0],fields[2]
            self.process_deferred_read({ 'identity': identity, 'message id': message_id })

            
    def process_lines_from_stream(self):
        '''process any lines from our streams that are available to read'''
        while True:
            try:
                l = self.input_stream.readline()
                self.process_line(l)
            except IOError:
                # Nothing left to read at the moment
                return


class ThroughputPrinter(object):
    def __init__(self, counter, outstream=sys.stdout, avgtimes=(600,60,1)):
        self.counter = counter
        self.outstream = outstream
        self.avgtimes = avgtimes
        self.lines_printed = 0

    def print_header(self):
        colbreak = " " * 3
        header = '#'
        header += "mean points per second".center(29) + colbreak
        header += "mean acks per second".center(30) + colbreak
        header += "mean latency per point".center(30) + colbreak

        header += "deferred points written/s".center(30) + colbreak
        header += "deferred points read/s".center(30) + colbreak
        header += "points timed out sending/s".center(30) + colbreak
        header += "unacked".rjust(10) + '\n'


        header += "#"
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)[1:]
        header += colbreak
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)
        header += colbreak
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)
        header += colbreak
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)
        header += colbreak
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)
        header += colbreak
        header += "".join(("(%dsec)" % secs).rjust(10) for secs in self.avgtimes)
        header += colbreak + "points".rjust(10) + '\n'

        header += '# ' + '-'*28 + colbreak + '-'*30 + colbreak + '-'*30 
        header += colbreak + '-'*30 + colbreak + '-'*30 + colbreak + '-'*30
        header += colbreak + '-'*10 + '\n'

        self.outstream.write(header)
        self.outstream.flush()

    def print_throughput(self):
        bursts_per_second = self.counter.get_points_per_seconds(self.avgtimes)
        acks_per_second = self.counter.get_acks_per_second(self.avgtimes)
        mean_latencies = self.counter.get_average_latencies(self.avgtimes)
        outstanding_points = self.counter.get_total_outstanding_points()
        points_deferred_to_disk = self.counter.get_deferred_points_written_per_second(self.avgtimes)
        points_read_from_disk = self.counter.get_deferred_points_read_per_second(self.avgtimes)
        points_timed_out_sending = self.counter.get_timed_out_points_per_second(self.avgtimes)

        # RENDER ALL THE THINGS!
        out = ""

        colbreak = " " * 3
        out += "".join((" %9.2f" % b for b in bursts_per_second))
        out += colbreak
        out += "".join((" %9.2f" % b for b in acks_per_second))
        out += colbreak
        out += "".join((" %9.2f" % b for b in mean_latencies))

        out += colbreak
        out += "".join((" %9.2f" % b for b in points_deferred_to_disk))
        out += colbreak
        out += "".join((" %9.2f" % b for b in points_read_from_disk))
        out += colbreak
        out += "".join((" %9.2f" % b for b in points_timed_out_sending))

        

        out += colbreak
        out += "%10d" % outstanding_points + '\n'

        if self.lines_printed % 20 == 0:
            self.print_header()

        self.outstream.write(out)
        self.outstream.flush()
        self.lines_printed += 1


if __name__ == '__main__':

    # Make stdin non-blocking
    fd = sys.stdin.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    reader = ThroughputCounter(sys.stdin)
    writer = ThroughputPrinter(reader, sys.stdout)
    
    # Run an event loop to process outstanding input every second
    # and then output the processed data

    event_loop = TimeAware(1, [ reader.process_lines_from_stream,
                                writer.print_throughput ])
    event_loop.run_forever()

# vim: set tabstop=4 expandtab:
