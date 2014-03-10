#!/usr/bin/env python2.6
import sys

'''watch a telemetry log and show how many databursts are outstanding over time

TTT 1393979427248973000 ffffffff messages_in = 68751
TTT 1393979427249019000 ffffffff acks_received_from_upstream = 68722
'''

from datetime import datetime
import time
import select


def outstanding_databursts_from_stream(stream):
	'''from an iterable of telemetry messages, yield outstanding databursts
	'''
	messages_in = 0
	for l in stream:
		if l[:4] != 'TTT ':
			continue
		fields = l.strip().split()
		if len(fields) < 6: 
			continue
		if fields[3] == 'messages_in':
			messages_in = int(fields[5])
		elif fields[3] == 'acks_received_from_upstream':
			acked = int(fields[5])
			timestamp = datetime.fromtimestamp(int(fields[1])/10**9)
			yield timestamp,messages_in-acked
			
def tail_file(f):
	while True:
		l = f.readline()
		if l == '':
			time.sleep(1)
		yield l

if __name__ == '__main__':
	if len(sys.argv) > 1:
		f = open(sys.argv[1],"r",1)
	else:
		f = open("/dev/stdin","r",1)
	tailf = tail_file(f)
	for timestamp,outstanding in outstanding_databursts_from_stream(tailf):
		print timestamp,'outstanding:',outstanding
		sys.stdout.flush()
