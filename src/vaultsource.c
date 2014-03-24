#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include "vaultreader.h"
#include "vaultsource.h"
#include "RequestMulti.pb-c.h"

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

/* on success returns a new RequestSource protobuf filled with source information
 *
 * WARNING: strings for key/value pairs vsource are only shallow copied
 *
 * on error returns NULL and sets errno
 * see also: free_requestsource_fb
 */
inline RequestSource * init_requestsource_pb(vsource_t *vsource, vtimestamp_t start_timestamp, vtimestamp_t end_timestamp) {
	int i;
	RequestSource *requestsource;
	RequestSource__Tag *tags;
	RequestSource__Tag **taglist;

	requestsource = malloc(sizeof(RequestSource));
	if (requestsource == NULL) return NULL;
	request_source__init(requestsource);

	/* Tag submessages themselves */
	tags = malloc(sizeof(RequestSource__Tag) * vsource->n_kvpairs);
	if (tags == NULL) { free(requestsource); return NULL; }

	/* Array of pointers to the same */
	taglist = malloc(sizeof(RequestSource__Tag *) * vsource->n_kvpairs);
	if (taglist == NULL) { free(requestsource); free(tags); return NULL; }
	requestsource->source = taglist;
	requestsource->n_source = vsource->n_kvpairs;

	for (i=0; i< vsource->n_kvpairs; ++i) {
		taglist[i] = tags+i;
		request_source__tag__init(taglist[i]);
		taglist[i]->field = vsource->keys[i];
		taglist[i]->value = vsource->values[i];
	}
	requestsource->alpha = start_timestamp;
	requestsource->alpha = end_timestamp;

	return requestsource;
}

/* frees memory for a protobuf created with init_requestsource_pb
 */
inline void free_requestsource_pb(RequestSource *requestsource) {
	free(requestsource->source[0]);	/* Free Tag submessages */
	free(requestsource->source);	/* Free list of pointers to Tag submessages */
	free(requestsource);		/* Free RequestSource message */
}
