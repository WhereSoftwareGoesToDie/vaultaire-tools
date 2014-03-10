#!/usr/bin/env python
import sys

'''from libmarquise telemetry list databursts that we been given that are not yet in the vault

TTT 1393898281196005000 a012ac63 collator_thread created_databurst frames = 1610
compressed_bytes = 18438
TTT 1393897260512605000 1a6809ff poller_thread rx_ack_from broker msg_id = 14536
'''

from datetime import datetime

if __name__ == '__main__':
    outstanding = {}
    for l in sys.stdin:
        if l[:4] != 'TTT ': 
            continue
        fields = l.strip().split()
        event = " ".join(fields[4:6])
        timestamp = int(fields[1])
        burst = fields[2]
        if event == 'created_databurst frames':
            outstanding[burst] = timestamp
        elif event == 'rx_act_from broker':
            del(outstanding[burst])
        last_event = timestamp/1000000000

    for burst,timestamp in outstanding.iteritems():
        timestamp /= 1000000000
        print burst,'created',datetime.fromtimestamp(timestamp),'-',(last_event-timestamp),'seconds outstanding'
