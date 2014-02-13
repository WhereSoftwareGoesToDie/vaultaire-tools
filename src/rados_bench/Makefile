CC=gcc
CFLAGS=-g -Wall -O2
LDFLAGS=-lrados

.PHONY: default
default: rados_bench

rados_bench: rados_bench.c

.PHONY: clean
clean:
	rm -f rados_bench

.PHONY: dev
dev: rados_bench
	scp rados_bench sio@cloud1.syd4:~/bin

.PHONY: prod
prod: rados_bench
	scp rados_bench sio@vaultaire-02.syd1:~/bin
