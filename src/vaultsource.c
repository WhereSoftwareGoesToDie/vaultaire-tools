#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include "vaultreader.h"
#include "vaultsource.h"

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
int tokenise_source(char *source, int source_len, vsource_t *vsource) {
	char * keys_arr[MAX_SOURCE_KVPAIRS];
	char * values_arr[MAX_SOURCE_KVPAIRS];
	char **keys = keys_arr; 
	char **values = values_arr;

	char *endp = source + source_len;
	char *sstart = source;
	char nexttok = '=';
	int kvpairs = 0;
	for (;source < endp && kvpairs < MAX_SOURCE_KVPAIRS; ++source) {
		if (*source == nexttok) {
			*source = 0;
			if (nexttok == '=') {
				*(keys++) = sstart;
				sstart = source+1;
				nexttok = ',';
			}
			else if (nexttok == ',') {
				*(values++) = sstart;
				sstart = source+1;
				nexttok = '=';
				kvpairs++;
			}
			else { assert(0); }
		}

	}
	if (nexttok == ',') {
		*values = sstart; 
		kvpairs++;
	}

	/* Now we know how many kv pairs there are, allocate and fill vsource */
	if (! _VSOURCE_ALLOC(*vsource,kvpairs))
		return -1;
	memcpy(vsource->keys, keys_arr, sizeof(char *) * kvpairs);
	memcpy(vsource->values, values_arr, sizeof(char *) * kvpairs);

	return kvpairs;
}
