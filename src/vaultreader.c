#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <zmq.h>

#include "vaultreader.h"
#include "vaultsource.h"

/* Initialise the vaultaire reader
 *
 * returns the reader context on success otherwise returns NULL */
void * init_vaultaire_reader() {
	return zmq_ctx_new();
}

/* Open a new connection to vaultaire for reading
 *
 * returns a reader connection on success otherwise returns NULL and sets errno */
void * vaultaire_reader_connect(void * reader_context, char *server_hostname) {
	void *socket;
	char socketname[1024];

	snprintf(socketname, 1024, "tcp://%s:5570/", server_hostname);
	socketname[1023] = 0;

	socket = zmq_socket(reader_context, ZMQ_REQ);
	if (socket == NULL) return NULL;
	if (zmq_connect(socket,socketname)) return NULL;

	return socket;
}


/* Close a vaultaire reader connection */
void vaultaire_reader_close(void * reader_connection) {
	zmq_close(reader_connection);
}

/* Shut down the vaultaire reader.
 *
 * All reader connections should be closed before this is called
 */
void vaultaire_reader_shutdown(void * reader_context) {
	while (zmq_ctx_term(reader_context) && errno == EINTR)
		;
}


/* Send a request for sources to the vaultaire broker
 * on failure returns -1 and sets errno
 */
int vaultaire_request_sources(void *reader_connection, char *origin, vsource_t **vsources, int nsources, vtimestamp_t timestamp) {
	return -1;
}


/* Read from the vault
 * sets errno returns 0 on failure;
 */
int vaultaire_read_sources(void *reader_connection, char *origin, char **sources, int nsources, vtimestamp_t timestamp) {
	errno = EINVAL;
	return 0;
}

int vaultaire_read_source(void *reader_connection, char *origin, char *source, vtimestamp_t timestamp) {
	int i;
	vsource_t vsource;


	if (tokenise_source(source, strnlen(source,MAX_SOURCE_LEN), &vsource)  < 1)
		{ errno = EINVAL; return -1; }


	for (i=0; i<vsource.n_kvpairs; ++i) {
		printf("('%s' => '%s') ", vsource.keys[i], vsource.values[i]);
	}
	return 0;
}

#if 0
void vaultaire_read_source(void *reader_connection, char *origin, char *source, uint64_t timestamp) {
	vaultaire_read_sources(reader_connection, origin, &source, 1, timestamp)
}
#endif
