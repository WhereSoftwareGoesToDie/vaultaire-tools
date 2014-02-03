/*
 * burstnetsink - Pretend to be a vaultaire broker and stream
 * 		  received DataBursts to stdout.
 *
 * output format is the DataBurst length as a network byte ordered uint32
 */
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <endian.h>
#include <assert.h>
#include <zmq.h>
#include <lz4.h>

#include "DataFrame.pb-c.h"
#include "DataBurst.pb-c.h"

#define DEBUG

#ifdef DEBUG
#define DEBUG_PRINTF(...) fprintf(stderr, __VA_ARGS__);
#else
#define DEBUG_PRINTF(...)
#endif

#define INITIAL_DECOMPRESS_BUFSIZE 1024000

/* write out a DataBurst
 *
 * First writes out the length of the databurst as a network
 * byte ordered uint32
 *
 * returns >= 1 on success
 */
int write_burst(FILE *fp, uint8_t *packed_burst, size_t packed_size) {
	int n_burstlen;

	n_burstlen = htonl(packed_size);
	if (fwrite(&n_burstlen, sizeof(n_burstlen), 1, fp) != 1)
		return -1;
	return fwrite(packed_burst, packed_size, 1, fp);
}

int main(int argc, char **argv) {
	void *zmq_context = zmq_ctx_new();
	void *zmq_responder = zmq_socket(zmq_context, ZMQ_ROUTER);
	uint8_t *decompressed_buffer;
	size_t decompressed_bufsize = INITIAL_DECOMPRESS_BUFSIZE;

	if (argc < 2) {
		fprintf(stderr, "%s <zmq socket to bind to>\n", argv[0]);
		return 1;
	}


	/* Need some space to decompress the databursts into
	 */
	decompressed_buffer = malloc(decompressed_bufsize);
	if (decompressed_buffer == NULL)
		return perror("malloc"), 1;

	/* Bind the zmq socket  */
	if (zmq_bind(zmq_responder, argv[1])) {
		return perror("zmq_bind"), 1;
	}

	/*
	 * Receive handler.
	 *
	 * 	* receive the message
	 * 	* write out
	 * 	* ack once write successful
	 */
	while(1) {
        zmq_msg_t ident, msg_id, burst;
		zmq_msg_init(&ident);
		zmq_msg_init(&msg_id);
		zmq_msg_init(&burst);

		size_t ident_rx  = zmq_msg_recv(&ident, zmq_responder, 0);
        if (ident_rx < 1 ) return perror("zmq_msg_recv (ident_rx)"), 1;

		size_t msg_id_rx = zmq_msg_recv(&msg_id, zmq_responder, 0);
        if (msg_id_rx < 1 ) return perror("zmq_msg_recv (msg_id_rx)"), 1;

		size_t burst_rx  = zmq_msg_recv(&burst, zmq_responder, 0);
        if (burst_rx < 1 ) return perror("zmq_msg_recv (burst_rx)"), 1;

        uint8_t *compressed_buffer;
        uint32_t uncompressed_size_from_header;
        uint32_t compressed_size_from_header;

        /* The databurst has an 8 byte header. 2 uint32s with
        * little endian ordering advising compressed and
        * decompressed size for some lz4 implementations.
        *
        * We can ignore this as we don't need to know the original
        * size to decompress
        */
        if (zmq_msg_size(&burst) <= 8) {
                fprintf(stderr, "received message too small. ignoring\n");
                zmq_msg_close(&burst);
                continue;
        }
        compressed_buffer = (uint8_t *)zmq_msg_data(&burst);

        uncompressed_size_from_header = le32toh(*(uint32_t *)compressed_buffer);
        compressed_buffer += sizeof(uint32_t);

        compressed_size_from_header = le32toh(*(uint32_t *)compressed_buffer);
        compressed_buffer += sizeof(uint32_t);

        DEBUG_PRINTF("uncompressed size from header %u\n",
                uncompressed_size_from_header);
        DEBUG_PRINTF("compressed size from header %u\n",
                compressed_size_from_header);

        if (zmq_msg_size(&burst) != (compressed_size_from_header + 8)) {
                fprintf(stderr, "Message size and header payload size don't match. skipping");
                zmq_msg_close(&burst);
                continue;
        }

        assert(((uint8_t *)zmq_msg_data(&burst) + 8) == compressed_buffer);

        /* Make sure we have enough room to decompress the burst into.
        *
        * We probably shouldn't trust the burst header here if this is
        * used in production as it could easily be used to DoS based on
        * memory usage.  At the same time, databursts can legitimately
        * be hundreds of MB in size, so limiting this is curious.
        */
        if (decompressed_bufsize < uncompressed_size_from_header) {
                void *new_buffer;
                DEBUG_PRINTF("growing buffer from %lu to %u bytes\n",
                        decompressed_bufsize,
                        uncompressed_size_from_header);
                new_buffer = realloc(decompressed_buffer, uncompressed_size_from_header);
                if (new_buffer == NULL) return perror("realloc"), 1;

                decompressed_buffer = new_buffer;
                decompressed_bufsize = uncompressed_size_from_header;
        }

        /* Decompress the databurst */
        int databurst_size;
        databurst_size = LZ4_decompress_safe(
                (const char *)compressed_buffer,
                (char *)decompressed_buffer,
                (int)compressed_size_from_header,
                (int)decompressed_bufsize);

        if (databurst_size < 1) {
                fprintf(stderr,"DataBurst decompression failure");
                return 1;
        }

        /* Crosscheck decompressed size is what we expect */
        if (databurst_size != uncompressed_size_from_header) {
                fprintf(stderr, "uncompressed DataBurst size and header don't match. skipping");
                zmq_msg_close(&burst);
                continue;
        }

        /* Write out and flush */
        if (write_burst(stdout, decompressed_buffer, databurst_size) < 0)
                return perror("writing databurst"), 1;

        fflush(stdout);
        zmq_msg_close(&burst);

        /* Send an ack */
        if(zmq_msg_send(&ident, zmq_responder, ZMQ_SNDMORE) < 0)
                return perror("zmq_send (ident)"), 1;

        if(zmq_msg_send(&msg_id, zmq_responder, ZMQ_SNDMORE) < 0)
                return perror("zmq_send (msg_id)"), 1;

        if (zmq_send(zmq_responder, NULL, 0, 0) < 0)
                return perror("zmq_send (null ack)"), 1;
	}

	free(decompressed_buffer);

	return 0;
}
