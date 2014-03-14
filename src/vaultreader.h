#ifndef __VAULTREADER_H
#define __VAULTREADER_H

#define MAX_SOURCE_KVPAIRS	128
#define	MAX_SOURCE_LEN		4096

/***********/

/* A vaultaire source, as keys and value pairs */
typedef struct {
	char **keys;
	char **values;
	int n_kvpairs;
} vsource_t;

/* Vaultaire timestamp is a uint64 in nanoseconds */
typedef uint64_t vtimestamp_t;

#define _VSOURCE_ALLOC(_vsource,_npairs) ( \
((_vsource).n_kvpairs = _npairs) &&\
((_vsource).keys = malloc(sizeof(char *)*(_npairs))) != NULL &&\
((_vsource).values = malloc(sizeof(char *)*(_npairs))) != NULL)
#define _VSOURCE_FREE(_vsource) { free((_vsource).keys); free((_vsource_values)); }


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

/* Read from the vault
 */
int vaultaire_read_source(void *reader_connection, char *origin, char *source, vtimestamp_t timestamp);
int vaultaire_read_sources(void *reader_connection, char *origin, char **sources, int nsources, vtimestamp_t timestamp);

#endif
