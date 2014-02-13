#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#include <rados/librados.h>

#include "words.h"

#define bail_if( assertion, message ) do { \
        if ( assertion ) {                 \
                perror( message );         \
                exit( 1 );                 \
        }                                  \
} while( 0 )


char get_envvar_int(const char *name, int *v) {
        char *var = getenv(name);
        if (!var) {
                return 0;
        }
        errno = 0;
        *v = strtol(var, NULL, 10);
        if (errno != 0 || *v == 0) {
                return -1;
        }
        return 1;
}

void init_rados(rados_t *cluster, rados_ioctx_t *io) {
        int err;
        char *user = getenv("RADOS_USER");
        bail_if(!user, "Must set RADOS_USER");
        err = rados_create(cluster, user);
        bail_if(err < 0, "rados_create");
        err = rados_conf_read_file(*cluster, "/etc/ceph/ceph.conf");
        bail_if(err < 0, "rados_conf_read_file: ");
        err = rados_connect(*cluster);
        bail_if(err < 0, "rados_connect: ");
        char *pool = getenv("RADOS_POOL");
        bail_if(!pool, "Must set RADOS_POOL");
        err = rados_ioctx_create(*cluster, pool, io);
        bail_if(err < 0, "rados_ioctx_create: ");
}

void init_oids(int n, char **oids, int *oid_len) {
        int i;
        srand(time(NULL));
        for (i = 0; i < n; i++) {
                int j = rand() % word_list_size; // I know why this is wrong.
                oids[i] = malloc(MAX_OID_SIZE);
                sprintf(oids[i], "bench_rados_%lld_%s", (long long)time(NULL), word_list[j]);
                oid_len[i] = strlen(oids[i]);
        }
}

void cleanup_oids(int n, char **oids, int *oid_len) {
        int i;
        for (i = 0; i < n; i++) {
                free(oids[i]);
        }
        free(oid_len);
        free(oids);
}

void write_oid(rados_ioctx_t *io, char *oid, int nwrites, int (*wait)(rados_completion_t)) {
        int i;
        int ret;
        printf("%s\n", oid);
        for (i = 0; i < nwrites; i++) {
                rados_completion_t comp;
                ret = rados_aio_create_completion(NULL, NULL, NULL, &comp);
                bail_if(ret < 0, "rados_aio_create_completion");
                rados_aio_append(*io, oid, comp, "four", 4);
                (*wait)(comp);
        }
}

int main() {
        int ret;
        int num_oids;
        int num_writes;
        int writes_per_oid;
        time_t write_start;
        time_t write_end;
        int i;
        char **oids;
        int *oid_len;
        rados_ioctx_t io;
        rados_t cluster;

        init_rados(&cluster, &io);

        ret = get_envvar_int("RADOS_NUM_OIDS", &num_oids);
        bail_if(ret != 1, "Must set RADOS_NUM_OIDS to an integral value");
        ret = get_envvar_int("RADOS_NUM_WRITES", &num_writes);
        bail_if(ret != 1, "Must set RADOS_NUM_WRITES to an integral value");

        writes_per_oid = num_writes / num_oids;
        if (num_writes % num_oids != 0) {
                printf("Warning: num_oids is not a factor of num_writes.\n");
        }
         
        oids = malloc(num_oids * sizeof(char*));
        oid_len = malloc(num_oids * sizeof(int));
        init_oids(num_oids, oids, oid_len);

        write_start = time(NULL);
        for (i = 0; i < num_oids; i++) {
                write_oid(&io, oids[i], writes_per_oid, rados_aio_wait_for_complete);
        }
        write_end = time(NULL);
        printf("%f\n", difftime(write_end, write_start));
        for (i = 0; i < num_oids; i++) {
                rados_remove(io, oids[i]);
        }
       
        cleanup_oids(num_oids, oids, oid_len); 
        rados_ioctx_destroy(io);
        rados_shutdown(cluster);
        
        return 0;
}
        
