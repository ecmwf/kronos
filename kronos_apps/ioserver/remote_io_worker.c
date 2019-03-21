
#include "common/logger.h"
#include "common/json.h"
#include "common/network/network.h"
#include "ioserver/io_task.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>


int main(int argc, char **argv) {

  int portno; /* port to listen on */

  NetMessage* msg;
  NetConnection* conn_with_client;
  Server* srv;
  int client_conn_fd;

  /* IO task functor */
  IOTaskFunctor* iotaskfunct;

  /* if io_taks ends with an error */
  int err_iotask;

  /* check command line arguments */
  if (argc != 2) {
    ERRO2("usage: %s <port>", argv[0]);
  }
  portno = atoi(argv[1]);

  /* create the server */
  srv = create_server(&portno);

  /* listening.. */
  net_listen(srv);

  /* preallocate client connection */
  conn_with_client = malloc(sizeof(NetConnection));

  /* main loop */
  INFO1("Server running..");
  while (1) {

    /* wait for incoming connections */
    client_conn_fd = net_accept(srv);
    if (client_conn_fd < 0){
        FATL1("error in accepting connection to server");
    }

    /* setup client connection */
    conn_with_client->socket_fd = client_conn_fd;
    conn_with_client->isConnectionOpen = 1;
    DEBG1("filled up conn struct");

    /* receive one message only */
    msg = recv_net_msg(conn_with_client);

    /* acknowledge reception */
    acknowledge_reception(conn_with_client);

    /* decide to honour a termination request (when arrives) */
    if (check_termination_msg(msg)){
        INFO1("received kill signal => terminating.");
        close(conn_with_client->socket_fd);
        return 0;
    }

    /* perform the IO task as requested */
    iotaskfunct = iotask_factory_from_string(msg->head);
    err_iotask = iotaskfunct->execute(iotaskfunct->data);
    if (err_iotask){
        ERRO1("reported error in executing IO task!");
    }

    /*
    * =====================================
    * TODO: actually move written/read data
    * =====================================
    */

    /* close connection */
    DEBG2("closing connection fd %i", client_conn_fd);
    close(client_conn_fd);

  } /* server loop */

  /* should's get here anyway.. */
  free(srv);
  free(conn_with_client);

}


