/*
 * burstnetsink - Pretend to be a vaultaire broker and stream
 *		  received DataBursts to stdout.
 *
 *		  Can also pretend to be an ingestd (dangerous) or snif the
 *		  telemetry socket of an existing broker (passive)
 *
 * output format is the DataBurst length as a network byte ordered uint32
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
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
	void *zmq_sock;
	uint8_t *decompressed_buffer;
	size_t decompressed_bufsize = INITIAL_DECOMPRESS_BUFSIZE;
	int verbose = 0;
	int hexdump = 0;
	int dummy_mode = 0;
	int slow_mode = 0;
	int broker_sub = 0;
	int fake_ingestd = 0;
	char *zmq_sock_address;

#define verbose_printf(...) { if (verbose) fprintf(stderr, __VA_ARGS__); }

	if (argc < 2) {
		fprintf(stderr, "%s [-v] [-x] <zmq socket>\n\n"
				"\t\t-v\tverbose\n"
				"\t\t-x\toutput databurst as hex\n"
				"\t\t-d\tdummy mode. ack messages but do not"
				" decompress, check or write to stdout\n"
				"\t\t-s\tslow mode. wait 1 second before acking\n"
				"\t\t-b\tconnect to the telemetry port of a broker"
				" rather than listening\n"
				"\t\t-i\tconnect to the ingestd (outgoing) port of a broker"
				" rather than listening\n\t\t\tWARNING: THIS WILL ACK AND DESTROY"
				" ANY FRAMES THAT IT RECEIVES THAT WERE DESTINED FOR VAULTAIRE\n"
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
		else if (strncmp("-s", *argv, 3) == 0)
			slow_mode = 1;
		else if (strncmp("-b", *argv, 3) == 0)
			broker_sub = 1;
		else if (strncmp("-i", *argv, 3) == 0)
			fake_ingestd =  1;
		else break;
		argv++; argc--;
	}
	zmq_sock_address = *argv;


	/* Need some space to decompress the databursts into
	 */
	decompressed_buffer = malloc(decompressed_bufsize);
	if (decompressed_buffer == NULL)
		return perror("malloc"), 1;

	if (broker_sub) {
		/* subscribe to the broker socket */
		verbose_printf("connecting/subscribing to %s\n", zmq_sock_address);
		zmq_sock = zmq_socket(zmq_context, ZMQ_SUB);
		if (zmq_connect(zmq_sock, zmq_sock_address))
			return perror("zmq_connect"), 1;
		if (zmq_setsockopt(zmq_sock, ZMQ_SUBSCRIBE, "", 0))
			return perror("zmq_setsockopt (subscribe)"), 1;
	}
	else if (fake_ingestd) {
		/* subscribe to the broker socket */
		verbose_printf("connecting as ingestd to %s\n", zmq_sock_address);
		zmq_sock = zmq_socket(zmq_context, ZMQ_ROUTER);
		if (zmq_connect(zmq_sock, zmq_sock_address))
			return perror("zmq_connect"), 1;
	}
	else {
		/* Bind the zmq socket to listen */
		verbose_printf("binding to %s\n", zmq_sock_address);
		zmq_sock = zmq_socket(zmq_context, ZMQ_ROUTER);
		if (zmq_bind(zmq_sock, zmq_sock_address)) {
			return perror("zmq_bind"), 1;
		}
	}

	/*
	 * Receive handler.
	 *
	 *	* receive the message
	 *	* write out
	 *	* ack once write successful
	 */
	while(1) {
		zmq_msg_t ident, msg_id, burst;
		zmq_msg_init(&ident);
		zmq_msg_init(&msg_id);
		zmq_msg_init(&burst);

		size_t ident_rx;
		errno = 0;
		do { ident_rx  = zmq_msg_recv(&ident, zmq_sock, 0);
		} while (errno == EINTR);
		if (ident_rx < 1 ) return perror("zmq_msg_recv (ident_rx)"), 1;
		if (!zmq_msg_more(&ident)) {
			fprintf(stderr, "Got short message (only 1 part). Skipping");
			zmq_msg_close(&ident);
			continue;
		}

		size_t msg_id_rx;
		errno = 0;
		do { msg_id_rx = zmq_msg_recv(&msg_id, zmq_sock, 0);
		} while (errno == EINTR);
		if (msg_id_rx < 1 ) return perror("zmq_msg_recv (msg_id_rx)"), 1;
		if (!zmq_msg_more(&msg_id)) {
			fprintf(stderr, "Got short message (only 2 parts). Skipping");
			zmq_msg_close(&ident); zmq_msg_close(&msg_id);
			continue;
		}

		size_t burst_rx;
		errno = 0;
		do { burst_rx = zmq_msg_recv(&burst, zmq_sock, 0);
		} while (errno == EINTR);
		if (burst_rx < 0 ) return perror("zmq_msg_recv (burst_rx)"), 1;
		if (broker_sub && burst_rx == 0) {
			if (verbose) {
				fprintf(stderr,"got ingestd ACK\n\tidentity:\t0x");
				fhexdump(stderr, zmq_msg_data(&ident), zmq_msg_size(&ident));
				fprintf(stderr, "\n\tmessage id:\t0x");
				fhexdump(stderr, zmq_msg_data(&msg_id), zmq_msg_size(&msg_id));
				fputc('\n', stderr);
			}
			zmq_msg_close(&ident); zmq_msg_close(&msg_id); zmq_msg_close(&burst);
			continue;
		}

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
			fprintf(stderr, "Got short message (small payload). Skipping\n");
			zmq_msg_close(&ident); zmq_msg_close(&msg_id); zmq_msg_close(&burst);
			continue;
		}
		compressed_buffer = (uint8_t *)zmq_msg_data(&burst);

		uncompressed_size_from_header = le32toh(*(uint32_t *)compressed_buffer);
		compressed_buffer += sizeof(uint32_t);

		compressed_size_from_header = le32toh(*(uint32_t *)compressed_buffer);
		compressed_buffer += sizeof(uint32_t);

		verbose_printf("\tcompressed:\t%u bytes\n\tuncompressed:\t%u bytes\n",
			compressed_size_from_header,
			uncompressed_size_from_header);

		if (zmq_msg_size(&burst) != (compressed_size_from_header + 8)) {
			fprintf(stderr, "Message size and header payload size don't match. skipping");
			zmq_msg_close(&ident); zmq_msg_close(&msg_id); zmq_msg_close(&burst);
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
				zmq_msg_close(&ident); zmq_msg_close(&msg_id); zmq_msg_close(&burst);
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
		zmq_msg_close(&ident);
		zmq_msg_close(&msg_id);
		zmq_msg_close(&burst);

		/* Send back acks if we aren't passively listening */
		if (!broker_sub) {
			/* Send an ack */
			if (slow_mode)
				usleep(1000000);

			if(zmq_msg_send(&ident, zmq_sock, ZMQ_SNDMORE) < 0)
				return perror("zmq_send (ident)"), 1;
			if(zmq_msg_send(&msg_id, zmq_sock, ZMQ_SNDMORE) < 0)
				return perror("zmq_send (msg_id)"), 1;
			if (zmq_send(zmq_sock, NULL, 0, 0) < 0)
			return perror("zmq_send (null ack)"), 1;
		}
	}
	DEBUG_PRINTF("done\n");

	free(decompressed_buffer);

	return 0;
}
