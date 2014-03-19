/* marquise_telemetry - connect to a chateau broker and stream all marquise telemetry to stdout
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <zmq.h>

int main(int argc, char **argv) {
	char zmq_endpoint[256];
	char *broker_hostname;
	char *client_filter = "";
	void *zmq_context;
	void *broker_sock;
	
	if (argc<2) {
		fprintf(stderr, "marquise_telemetry <broker hostname> [client filter]\n");
		return EXIT_FAILURE;
	}
	broker_hostname = argv[1];
	if (argc > 2) 
		client_filter = argv[2];

	/* Make sure stdout is linebuffered */
	setvbuf(stdout, NULL, _IOLBF, 0);

	snprintf(zmq_endpoint, 256, "tcp://%s:5582/", broker_hostname);
	zmq_endpoint[255] = 0;

	/* Connect to broker and subscribe */
	zmq_context = zmq_ctx_new();
	if (zmq_context == NULL) 
		return perror("zmq_ctx_new"), EXIT_FAILURE;
	broker_sock = zmq_socket(zmq_context, ZMQ_SUB);
	if (broker_sock == NULL) 
		return perror("zmq_socket"), EXIT_FAILURE;
	if (zmq_connect(broker_sock, zmq_endpoint)) 
		return perror("zmq_connect"), EXIT_FAILURE;
	if (zmq_setsockopt(broker_sock, ZMQ_SUBSCRIBE, client_filter, 0))
		return perror("zmq_subscribe"), EXIT_FAILURE;

	/* Watch a while. Watch FOREVER */
	int ret = 0;
	while (1) {
		zmq_msg_t msg;
		zmq_msg_init(&msg);

		int rx;
		errno = 0;
		do { rx = zmq_msg_recv(&msg, broker_sock, 0);
		} while (errno == EINTR);
		if (rx < 0) {
			perror("zmq_msg_recv");
			ret = EXIT_FAILURE;
			break;
		}
		fwrite(zmq_msg_data(&msg), zmq_msg_size(&msg), 1,stdout);
		fputc('\n', stdout);
		fflush(stdout);
		zmq_msg_close(&msg);
	}
	zmq_close(broker_sock);
	zmq_ctx_term(zmq_context);
	return ret;
}
