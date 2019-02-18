
#include "io_executor.h"
#include "logger.h"
#include "kronos/json.h"

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

  int parentfd; /* parent socket */
  int childfd; /* child socket */
  int portno; /* port to listen on */
  socklen_t clientlen; /* byte size of client's address */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  struct hostent *hostp; /* client host info */

  char iotask_msg_buffer[JSON_BUF_LEN]; /* message buffer */
  char io_data[IO_DATA_BUF];
  int errno_io;

  char *hostaddrp; /* dotted decimal host addr string */
  int optval; /* flag value for setsockopt */
  int n; /* message byte size */
  const char *ack_msg = "message-processed";
  const char *kill_msg = "terminate-server";


  /*
   * check command line arguments
   */
  if (argc != 2) {
    ERRO2("usage: %s <port>", argv[0]);
  }
  portno = atoi(argv[1]);

  /*
   * socket: create the parent socket
   */
  parentfd = socket(AF_INET, SOCK_STREAM, 0);
  if (parentfd < 0)
    ERRO1("ERROR opening socket");

  /* setsockopt: Handy debugging trick that lets
   * us rerun the server immediately after we kill it;
   * otherwise we have to wait about 20 secs.
   * Eliminates "ERROR on binding: Address already in use" error.
   */
  optval = 1;
  setsockopt(parentfd, SOL_SOCKET, SO_REUSEADDR,
             (const void *)&optval , sizeof(int));

  /*
   * build the server's Internet address
   */
  memset((char *) &serveraddr, 0, sizeof(serveraddr));

  /* this is an Internet address */
  serveraddr.sin_family = AF_INET;

  /* let the system figure out our IP address */
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  INFO3("Server running on host: %s, port:%i",
         serveraddr.sin_addr.s_addr,
         portno);


  /* this is the port we will listen on */
  serveraddr.sin_port = htons((unsigned short)portno);

  /*
   * bind: associate the parent socket with a port
   */
  if (bind(parentfd, (struct sockaddr *) &serveraddr, (socklen_t)sizeof(serveraddr)) < 0) {
    ERRO1("ERROR on binding");
  }

  /*
   * listen: make this socket ready to accept connection requests
   */
  if (listen(parentfd, MAXNREQ) < 0)
    ERRO1("ERROR on listen");

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
    childfd = accept(parentfd, (struct sockaddr *) &clientaddr, &clientlen);
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

    /* perform the IO task as requested */
    errno_io = execute_io_task(iotask_msg_buffer, io_data);

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


