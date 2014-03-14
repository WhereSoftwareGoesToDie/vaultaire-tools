#ifndef __VAULTREADER_H
#define __VAULTREADER_H

#include "vaultsource.h"

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
 * Read multiple sources from the vault
 * sets errno and returns 0 on failure
 */
int vaultaire_read_sources(void *reader_connection, char *origin, 
				char **sources, int nsources,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp);

/*
 * Read a single source from the vault
 * sets errno and returns 0 on failure
 */
int vaultaire_read_source(void *reader_connection, char *origin, char *source,
				vtimestamp_t start_timestamp,
				vtimestamp_t end_timestamp);

#endif
