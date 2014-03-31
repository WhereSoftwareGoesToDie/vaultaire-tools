#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <endian.h>
#include <assert.h>
#include <lz4.h>
#include <errno.h>

#define DEBUG

#ifdef DEBUG
#define DEBUG_PRINTF(...) fprintf(stderr, __VA_ARGS__);
#define verbose_printf DEBUG_PRINTF
#else
#define DEBUG_PRINTF(...)
#endif


#define INITIAL_DECOMPRESS_BUFSIZE 1024000

/* Decompress a databurst.
 
 * on success returns amount of bytes decompressed.
 * returns -1 of failure and errno is set
 *
 * *output_buffer will be realloc()ed to grow to the required size
 * if needed, and *output_bufsize will be updated to the new size accordingly
 * This may be larger than the amount of bytes decompressed.
 *
 */
int decompress_databurst(uint8_t *input_buffer, size_t input_size, 
			 uint8_t **output_buffer, size_t *output_bufsize) {
	uint32_t uncompressed_size_from_header;
	uint32_t compressed_size_from_header;


	/* The databurst has an 8 byte header. 2 uint32s with
	 * little endian ordering advising compressed and
	 * decompressed size for some lz4 implementations.
	 *
	 * We can ignore this as we don't need to know the original
	 * size to decompress
	 */
	if (input_size < 8) {
		errno = EINVAL;
		return -1;
	}
	uncompressed_size_from_header = le32toh(*(uint32_t *)input_buffer);
	input_buffer += sizeof(uint32_t);

	compressed_size_from_header = le32toh(*(uint32_t *)input_buffer);
	input_buffer += sizeof(uint32_t);

	verbose_printf("\tcompressed:\t%u bytes\n\tuncompressed:\t%u bytes\n",
		compressed_size_from_header,
		uncompressed_size_from_header);

	if (input_size != (compressed_size_from_header + 8)) {
		errno = EINVAL;
		return -1;
	}

	/* Make sure we have enough room to decompress the burst into.
	*
	* We probably shouldn't trust the burst header here if this is
	* used in production as it could easily be used to DoS based on
	* memory usage.  At the same time, databursts can legitimately
	* be hundreds of MB in size, so limiting this is curious.
	*/
	if (*output_bufsize < uncompressed_size_from_header) {
		void *new_buffer;
		DEBUG_PRINTF("growing buffer from %lu to %u bytes\n",
			*output_bufsize,
			uncompressed_size_from_header);
		new_buffer = realloc(*output_buffer, uncompressed_size_from_header);
		if (new_buffer == NULL) return perror("realloc"), 1;

		*output_buffer = new_buffer;
		*output_bufsize = uncompressed_size_from_header;
	}

	/* Decompress the databurst */
	int databurst_size;
	databurst_size = LZ4_decompress_safe(
		(const char *)input_buffer,
		(char *)*output_buffer,
		(int)compressed_size_from_header,
		(int)*output_bufsize);

	if (databurst_size < 1) {
		fprintf(stderr,"DataBurst decompression failure");
		errno = ENOMSG;
		return 1;
	}

	/* Crosscheck decompressed size is what we expect */
	if (databurst_size != uncompressed_size_from_header) {
		fprintf(stderr, "uncompressed DataBurst size and header don't match");
		errno = EINVAL;
		return 1;
	}

	return databurst_size;
}
