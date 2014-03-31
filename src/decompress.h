#ifndef __DECOMPRESS_H
#define __DECOMPRESS_H

#include <stdint.h>

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
			 uint8_t **output_buffer, size_t *output_bufsize);

#endif
