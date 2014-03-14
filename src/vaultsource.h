#ifndef __VAULTSOURCE_H
#define __VAULTSOURCE_H

#include <stdint.h>

#define MAX_SOURCE_KVPAIRS	128
#define	MAX_SOURCE_LEN		4096

/* A vaultaire source, as keys and value pairs */
typedef struct {
	char **keys;
	char **values;
	int n_kvpairs;
} vsource_t;

/* Vaultaire timestamp is a uint64 in nanoseconds */
typedef uint64_t vtimestamp_t;

/* allocate key/value lists inside a vsource_t
 * Evaluates to true iff successful, otherwise errno is set
 */
#define _VSOURCE_ALLOC(_vsource,_npairs) ( \
((_vsource).n_kvpairs = _npairs) &&\
((_vsource).keys = malloc(sizeof(char *)*(_npairs))) != NULL &&\
((_vsource).values = malloc(sizeof(char *)*(_npairs))) != NULL)

/* free key/value lists inside a vsource_t */
#define _VSOURCE_FREE(_vsource) { free((_vsource).keys); free((_vsource_values)); }


/* tokenize a source into key value pairs
 *
 * sources are read in the form  of:
 *
 * 	key=value:key=value:key=value:....
 *
 * *vsource will be allocated with _VSOURCE_ALLOC and must be later freed with
 * _VSOURCE_FREE
 *
 * The source string will have NULLs inserted to create individual strings
 * from the source, and pointers to the start of these strings will be placed
 * inside vsource->keys and vsource->values
 *
 * tokenise_source returns the amount of key/value pairs tokenised
 * on success and on failure returns -1 and sets errno.
 */
int tokenise_source(char *source, int source_len, vsource_t *vsource);

#endif
