#ifndef __VAULTREADER_H
#define __VAULTREADER_H

#include "DataFrame.pb-c.h"
#include "DataBurst.pb-c.h"

#include "vaultsource.h"


typedef struct vaultaire_response {
	// FIXME: Only need a ll if we are rebuilding the frames themselves
	struct vaultaire_response *next;

	DataFrame **frames;
	size_t num_frames;

	/* Internal to vaultreader. Messing with these may lead to badness */
	DataBurst * _databurst;
} vaultaire_response_t;

/**********/

/* Initialise the vaultaire reader
 *
 * returns the reader context on success
 * otherwise returns NULL
 */
void * init_vaultaire_reader();

/* Open a new connection to vaultaire for reading
 *
 * returns a reader connection on success
 * otherwise returns NULL and sets errno
 */
void * vaultaire_reader_connect(void * reader_context, char *server_hostname);


/* Close a vaultaire reader connection
 */
void vaultaire_reader_close(void * reader_connection);

/* Shut down the vaultaire reader.
 *
 * All reader connections should be closed before this is called
 */

void vaultaire_reader_shutdown(void * reader_context);


/* 
 * Send a request to read multiple sources from the vault
 *
 * use vaultaire_read_responses(...) to retreive any responses
 * on success
 *
 * returns > 0 on success
 * sets errno and returns 0 on failure
 */
int vaultaire_request_sources(void *reader_connection, char *origin, 
				char **sources, int nsources,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp);

/*
 * Read a single source from the vault
 * Send a request to read a single source from the vault
 *
 * use vaultaire_read_responses(...) to retreive any responses
 * on success
 *
 * returns > 0 on success
 * sets errno and returns 0 on failure
 */
int vaultaire_request_source(void *reader_connection, char *origin, char *source,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp);

/* read back responses from a successful request to the vault
 *
 * After a successful call to vaultaire_request_source or 
 * vaultaire_request_sources repeatedly call vaultaire_read_responses
 * until it returns 0, indicating there are no responses left to be
 * read.
 *
 * **responses will be set to point to the head of a list of vaultaire_response_t
 * vaultaire_free_responses(vaultaire_response_t *) must be called with this pointer
 * once data has been processed by the application.
 *
 * returns the amount of responses read
 * sets errno and returns -1 on failure
 */
int vaultaire_read_responses(void *reader_connection, vaultaire_response_t **responses);

/* Free memory from responses read by vaultaire_read_responses */
void vaultaire_free_responses(vaultaire_response_t *responses);

#endif
