#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "vaultreader.h"

static void usage() {
	fprintf(stderr, "usage:\treadclient <vaultaire server> <origin>\n\n" \
		"Reads from stdin lines the form: \n" \
		"\n\t<source> <start_timestamp> <end_timestamp>\n" \
		"\nwhere <source> is of the form:\n" \
		"\n\tkey=value[,key=value[,key=value ...]]\n" \
		"\ne.g.:\n\n\tip=10.44.3.2,metric=bunnies_per_hour 1394077705 1394164114\n\n");
}

/* read a single line in the form:
 *
 *   <source> <start_timestamp> <end_timestamp>
 *
 * memory for the source string is allocated as needed, and
 * must be freed by the caller when no longer needed.
 *
 * memory for start_timestamp and end_timestamp is not allocated
 * and must be supplied by the caller
 */
static int read_query_line(FILE *fp, char **source, uint64_t *start_timestamp, uint64_t *end_timestamp) {
	return fscanf(fp,"%ms%lu%lu",source,start_timestamp,end_timestamp) == 3;
}


int main(int argc, char **argv) {
	char *vaultaire_server;
	char *origin;
	char *source;
	uint64_t start_timestamp;
	uint64_t end_timestamp;
	void *reader;
	void *reader_connection;

	if (argc < 3) {
		usage();
		return -1;
	}
	vaultaire_server = argv[1];
	origin = argv[2];

	/* Connect to vaultaire */
	void * init_vaultaire_reader();
	reader = init_vaultaire_reader();
	if (reader == NULL) 
		return perror("init_vaultaire_reader"), 1;

	reader_connection = vaultaire_reader_connect(reader, vaultaire_server);
	if (reader_connection == NULL) 
		return perror("vaultaire_reader_connect"), 1;


	/* Go through the sources */
	while (read_query_line(stdin,&source,&start_timestamp,&end_timestamp)) {
		/* Start and stop timestamp are in nanoseconds */
		start_timestamp *= 1000000000;
		end_timestamp *= 1000000000;

		int ret = vaultaire_request_source(reader_connection, origin, source, 
						start_timestamp, end_timestamp);
		if (ret < 0) 
			return perror("vaultaire_read_source"), 1;
		free(source);

		int n_read;
		do {
			vaultaire_response_t *responses;
			n_read = vaultaire_read_responses(reader_connection, &responses);
			if (n_read < 0) {
				perror("vaultaire_read_responses");
				break;
			}
			/* ... process responses ... */
			vaultaire_free_responses(responses);
		} while (n_read > 0);
	}


	/* Close down */
	vaultaire_reader_close(reader_connection);
	vaultaire_reader_shutdown(reader);

	return 0;
}
