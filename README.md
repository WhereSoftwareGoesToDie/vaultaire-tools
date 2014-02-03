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
