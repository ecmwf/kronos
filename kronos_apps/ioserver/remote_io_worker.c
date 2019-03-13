
#include "io_executor.h"
#include "common/logger.h"
#include "common/json.h"
#include "common/network/network.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>


/* max num of connection requests allowed */
#define MAXNREQ 1024
#define JSON_BUF_LEN 1024
#define IO_DATA_BUF 1048576

int main(int argc, char **argv) {

  int childfd; /* child socket */
  int portno; /* port to listen on */
  socklen_t clientlen; /* byte size of client's address */
  struct sockaddr_in clientaddr; /* client addr */
  struct hostent *hostp; /* client host info */
  char *hostaddrp;

  NetMessage* msg;
  NetConnection* conn;
  Server* srv;

  /*
   * check command line arguments
   */
  if (argc != 2) {
    ERRO2("usage: %s <port>", argv[0]);
  }
  portno = atoi(argv[1]);

  /* server setup */
  srv = setup_server(&portno);

  /* listen.. */
  net_listen(srv);


  /*
   * main loop: wait for a connection request, echo input line,
   * then close connection.
   */
  clientlen = sizeof(clientaddr);
  INFO1("Server running..");
  while (1) {

    /*
     * accept: wait for a connection request
     */
    childfd = accept(srv->socket_fd,
                     (struct sockaddr *)
                     &clientaddr, &clientlen);

    if (childfd < 0) {
      ERRO1("ERROR on accept");
    }

    /*
     * gethostbyaddr: determine who sent the message
     */
    hostp = gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr,
                          sizeof(clientaddr.sin_addr.s_addr), AF_INET);
    if (hostp == NULL) {
      ERRO1("ERROR on gethostbyaddr");
    }

    hostaddrp = inet_ntoa(clientaddr.sin_addr);
    if (hostaddrp == NULL) {
      ERRO1("ERROR on inet_ntoa");
    }

    INFO3("server established connection with %s (%s)", hostp->h_name, hostaddrp);

    /* receive one message only */
    conn = malloc(sizeof(NetConnection));
    strncpy(conn->host, "hello", 6);
    conn->port = portno;
    conn->socket_fd = childfd;
    conn->isConnectionOpen = 1;
    DEBG1("filled up conn struct");
    msg = recv_net_msg(conn);

#if 0
    memset(iotask_msg_buffer, 0, JSON_BUF_LEN);
    n = read(childfd, iotask_msg_buffer, JSON_BUF_LEN);
    if (n < 0)
      perror("ERROR reading from socket");

    INFO3("server received %d bytes, msg: %s", n, iotask_msg_buffer);

    if (!strcmp(iotask_msg_buffer, kill_msg)){
        INFO2("received kill signal %s => terminating.", iotask_msg_buffer);
        INFO1("Worker finished successfully.");
        return 0;
    }

    /*
     * acknowledge the client for reception
     */
    n = write(childfd, ack_msg, strlen(ack_msg));
    if (n < 0) {
      FATL1("ERROR writing to socket");
    }
#endif
    /* perform the IO task as requested */

    /*
    errno_io = execute_io_task_from_string(iotask_msg_buffer);
    */

    /* send data back (only if it's a reading instruction)
    if (!errno_io){
       n = write(childfd, ack_msg, strlen(ack_msg));
       if (n < 0)
         perror("ERROR writing to socket");
    } */


    /* close connection */
    close(childfd);

  } /* server loop */

}


