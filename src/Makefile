CC=gcc
CFLAGS=-g -Wall -O2
LDFLAGS=-lprotobuf-c -lrt
PROTOCC=protoc-c
PROTO_PATH=../protobuf/
PROTO_SRC=${PROTO_PATH}DataFrame.proto

.PHONY: default
default: framecat

DataFrame.pb-c.c: ${PROTO_SRC}
	${PROTOCC} --proto_path=${PROTO_PATH} ${PROTO_SRC} --c_out .

framecat: DataFrame.pb-c.c framecat.c

.PHONY: clean
clean:
	rm -f framecat.o DataFrame.pb-c.[coh] framecat

