/*
 * burstnetsink - Pretend to be a vaultaire broker and stream
 * 		  received DataBursts to stdout.
 *
 * output format is the DataBurst length as a network byte ordered uint32
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/* most inefficient hexdump on the planet */
void fhexdump(FILE *fp, uint8_t *buf, size_t bufsiz) { 
	while (bufsiz--)
		fprintf(fp,"%02x", *(buf++));
}

int main(int argc, char **argv) {
	void *zmq_context = zmq_ctx_new();
	void *zmq_responder = zmq_socket(zmq_context, ZMQ_ROUTER);
	uint8_t *decompressed_buffer;
	size_t decompressed_bufsize = INITIAL_DECOMPRESS_BUFSIZE;
	int verbose = 0;
	int hexdump = 0;
	int dummy_mode = 0;
	char *zmq_bind_address;

#define verbose_printf(...) { if (verbose) fprintf(stderr, __VA_ARGS__); }

	if (argc < 2) {
		fprintf(stderr, "%s [-v] [-x] <zmq socket to bind to>\n\n"
				"\t\t-v\tverbose\n"
				"\t\t-x\toutput databurst as hex\n"
				"\t\t-d\tdummy mode. ack messages but do not"
				"decompress, check or write to stdout\n"
				, argv[0]);
		return 1;
	}


	/* Parse command line 
	 */
	argv++; argc--;
	while (argc > 1) {
		if (strncmp("-v", *argv, 3) == 0) 
			verbose = 1;
		else if (strncmp("-x", *argv, 3) == 0) 
			hexdump = 1;
		else if (strncmp("-d", *argv, 3) == 0) 
			dummy_mode = 1;
		else break;
		argv++; argc--;
	}
	zmq_bind_address = *argv;


	/* Need some space to decompress the databursts into
	 */
	decompressed_buffer = malloc(decompressed_bufsize);
	if (decompressed_buffer == NULL)
		return perror("malloc"), 1;

	/* Bind the zmq socket  */
	verbose_printf("binding to %s\n", zmq_bind_address);
	if (zmq_bind(zmq_responder, zmq_bind_address)) {
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

	if (verbose) {
		fprintf(stderr, "received %lu bytes\n\tidentity:\t0x", zmq_msg_size(&burst));
		fhexdump(stderr, zmq_msg_data(&ident), zmq_msg_size(&ident));
		fprintf(stderr, "\n\tmessage id:\t0x");
		fhexdump(stderr, zmq_msg_data(&msg_id), zmq_msg_size(&msg_id));
		fputc('\n', stderr);
	}

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

        verbose_printf("\tcompressed:\t%u bytes\n\tuncompressed:\t%u bytes\n",
                uncompressed_size_from_header,
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

	if (!dummy_mode) {
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
		if (! hexdump) {
			if (write_burst(stdout, decompressed_buffer, databurst_size) < 0)
				return perror("writing databurst"), 1;
		}
		else {
			fhexdump(stdout, decompressed_buffer, databurst_size);
			printf("\n");
		}

		fflush(stdout);
	}
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
