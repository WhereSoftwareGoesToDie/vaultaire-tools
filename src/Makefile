CC=gcc
INSTALL?=install
PROTOCC=protoc-c

CFLAGS=-g -Wall -O2
LDFLAGS=-lprotobuf-c -lrt
PROTO_PATH=../protobuf/

PREFIX?=/usr/local
BINDIR?=$(PREFIX)/bin


.PHONY: default
default: framecat burstnetsink 

# protobufc
%.pb-c.c: ${PROTO_PATH}${@:.pb-c.c=.proto}
	${PROTOCC} --proto_path=${PROTO_PATH} ${PROTO_PATH}${@:.pb-c.c=.proto} --c_out .

framecat: DataFrame.pb-c.c 

LDFLAGS:=${LDFLAGS} -lzmq -llz4
burstnetsink: DataFrame.pb-c.c DataBurst.pb-c.c 

.PHONY: clean
clean:
	rm -f framecat.o DataBurst.pb-c.[coh] DataFrame.pb-c.[coh] framecat burstnetsink


install: framecat burstnetsink
	$(INSTALL) framecat $(DESTDIR)$(BINDIR)
	$(INSTALL) burstnetsink $(DESTDIR)$(BINDIR)
