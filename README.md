vaultaire-tools
===============

tool-kit of command-line tools to work with vaultaire data

framecat:

	framecat takes vaultaire frames from stdin and output them in a human
	readable form

	The form framecat reads is a 4 byte uint32 in network byte order containing
	the length of the frame, followed by the frame itself. e.g.:

		[ 4 bytes containing length of frame 1 ]
		[ frame 1 ]
		[ 4 bytes containing length of frame 2 ]
		[ frame 2 ]
		...

	etc.

burstnetsink:

	burstnetsink listens on a zeromq socket and pretends to be a vaultaire
	broker, decompresses and writes any received DataBursts to stdout.

	It can be used to get databursts from any libmarquise client

	DataBursts are written out with the same length header format used
	by framecat

framefelid:

	framefelid is a reimplementation of framecat in go, with some
	extra features like JSON output. It supports reading DataBursts;
	unlike framecat it reads either a single DataFrame or a single
	DataBurst (pass -burst=false to read a Frame rather than a
	Burst). 

framegen:

	framegen generates DataFrames with random values for testing
	purposes. It can output either single DataFrames or DataBursts,
	and supports writing multiple DataBursts to sequentially-named
	files (it's not very smart with its memory handling, so this
	is necessary to output more DataFrames than you have available
	memory).

broker\_thoughput:

	Show throughput of frames passing through a broker to the ingestd
	as well as latency for frames to be acked back to the client.

