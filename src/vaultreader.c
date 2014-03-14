#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <zmq.h>

#include "vaultreader.h"
#include "vaultsource.h"

#include "RequestMulti.pb-c.h"

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
 * on success returns 0
 * on failure returns -1 and sets errno
 */
int vaultaire_request_sources(void *reader_connection, char *origin, vsource_t *vsources, int nsources, vtimestamp_t start_timestamp, vtimestamp_t end_timestamp) {
	RequestMulti request;
	RequestSource ** requestlist;
	int i;
	char *buf;
	size_t bufsiz;

	requestlist = malloc(sizeof(RequestSource *) * nsources);
	if (requestlist == NULL) return -1;

	request_multi__init(&request);
	request.requests = requestlist;
	request.n_requests = nsources;

	/* Build the submessages for each source request */
	for (i=0; i <nsources; ++i) {
		requestlist[i] = init_requestsource_pb(vsources+i, start_timestamp, end_timestamp);
	}

	/* Pack the protobuf including all submessages */
	bufsiz = request_multi__get_packed_size(&request);
	buf = malloc(bufsiz);
	if (buf == NULL) {
	       for (i=0; i<nsources; ++i)
	       	       free_requestsource_pb(requestlist[i]); 
	       return -1;
	}

	request_multi__pack(&request, (uint8_t *)buf);

	/* Send it out onto the wire */
	int ret;
	errno = 0;
	do {
		ret = zmq_send(reader_connection, buf, bufsiz,0);
	} while (ret == -1 && errno == EINTR);

	/* Free the packed protobuf, submessages and list of submessages */
	for (i=0; i<nsources; ++i)
		free_requestsource_pb(requestlist[i]);
	free(requestlist);
	free(buf);

	/* zmq_send would have set -1 on failure */
	return (ret == -1 ? -1 : 0);
}

int vaultaire_request_source(void *reader_connection, char *origin, vsource_t *vsource,vtimestamp_t start_timestamp, vtimestamp_t end_timestamp) {
	return vaultaire_request_sources(reader_connection,origin, vsource, 1, start_timestamp, end_timestamp);
}


/* 
 * Read multiple sources from the vault
 *
 * WARNING: Currently destructive the the strings in the sources list
 *
 * sets errno and returns -1 on failure
 */
int vaultaire_read_sources(void *reader_connection, char *origin, 
				char **sources, int nsources,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp) {
	vsource_t * vsource_list;
	int i;
	int ret;

	/* Tokenise all sources */
	vsource_list = malloc(sizeof(vsource_t) * nsources);
	if (vsource_list == NULL) return -1;

	for (i=0; i<nsources; ++i) {
		ret = tokenise_source(	sources[i], 
					strnlen(sources[i],MAX_SOURCE_LEN), 
					vsource_list+i);
		if (ret < 1) {
			/* Free all the things! */
			while (--i >= 0)
				_VSOURCE_FREE(vsource_list[i]);
			errno = EINVAL;
			return -1;
		}
	}

	/* Send the request to vaultaire for the sources*/
	ret = vaultaire_request_sources(reader_connection, origin,
					vsource_list, nsources,
					start_timestamp, end_timestamp);

	/* TODO: READ THE RESPONSES */

	/* Free tokenised source data and vsource_list */
	for (i=0; i<nsources; ++i) 
		_VSOURCE_FREE(vsource_list[i]);
	free(vsource_list);
	return 0;
}


/*
 * Read a single source from the vault
 * sets errno and returns 0 on failure
 */
int vaultaire_read_source(void *reader_connection, char *origin, char *source,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp) {
	vsource_t vsource;

	//int vaultaire_request_sources(void *reader_connection, char *origin, vsource_t **vsources, int nsources, vtimestamp_t start_timestamp, vtimestamp end_timestamp) {
	if (tokenise_source(source, strnlen(source,MAX_SOURCE_LEN), &vsource)  < 1)
		{ errno = EINVAL; return -1; }

	int ret = vaultaire_request_source(reader_connection, origin, &vsource, start_timestamp, end_timestamp);

	_VSOURCE_FREE(vsource);
	return ret;
}

#if 0
void vaultaire_read_source(void *reader_connection, char *origin, char *source, uint64_t timestamp) {
	vaultaire_read_sources(reader_connection, origin, &source, 1, timestamp)
}
#endif
