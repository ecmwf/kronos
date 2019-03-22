
#include "common/logger.h"
#include "common/json.h"
#include "common/network/network.h"
#include "ioserver/io_task.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>


#define SRV_OFF_CODE -888


/* execute the IO task */
int execute_io_task(NetMessage* msg){

    /* IO task functor */
    IOTaskFunctor* iotaskfunct;

    /* if io_taks ends with an error */
    int err_iotask = 0;

    if (msg->head_len){
        DEBG2("asked to perform task: %s", msg->head);
        iotaskfunct = iotask_factory_from_string(msg->head);
        err_iotask = iotaskfunct->execute(iotaskfunct->data);
        if (err_iotask){
            ERRO1("reported error in executing IO task!");
        }
    } else {
        ERRO1("Empty message received!");
    }

    return err_iotask;
}


int handle_connection(int conn){

    NetMessage* msg;
    NetConnection* conn_with_client;

    /* preallocate client connection */
    conn_with_client = malloc(sizeof(NetConnection));

    /* setup client connection */
    conn_with_client->socket_fd = conn;
    conn_with_client->isConnectionOpen = 1;
    DEBG1("filled up conn struct");

    /* receive one message only */
    msg = recv_net_msg(conn_with_client);

    /* acknowledge reception */
    acknowledge_reception(conn_with_client);

    /* honour a termination request (when arrives) */
    if (check_termination_msg(msg)){
        INFO1("received kill signal => terminating.");
        close(conn_with_client->socket_fd);
        return SRV_OFF_CODE;
    }

    /* perform the IO task as requested */
    if (execute_io_task(msg) < 0){
      ERRO1("error executing the task!");
    }

    /*
    * =====================================
    * TODO: actually move written/read data
    * =====================================
    */

    /* close connection (only 1 msg from each conn) */
    DEBG2("closing connection fd %i", conn);
    close(conn);

    /* remove connection */
    free(conn_with_client);

    return 0;
}

/* minimal server that handles IO-task requests */
int main(int argc, char **argv) {

  int portno; /* port to listen on */

  Server* srv;
  int client_conn_fd;
  int srv_run_code = 1;

  /* check command line arguments */
  if (argc != 2) {
    ERRO2("usage: %s <port>", argv[0]);
  }
  portno = atoi(argv[1]);

  /* create the server */
  srv = create_server(&portno);

  /* listening.. */
  net_listen(srv);

  /* main loop */
  INFO1("Server running..");
  while (srv_run_code != SRV_OFF_CODE) {

    /* wait for incoming connections */
    client_conn_fd = net_accept(srv);
    if (client_conn_fd < 0){
        FATL1("error in accepting connection to server");
    }

    /* handle the connection */
    srv_run_code = handle_connection(client_conn_fd);

    if (srv_run_code == -1){
        ERRO1("iotask failed!");
    } else if (srv_run_code == SRV_OFF_CODE) {
        INFO1("terminating server");
    }

  } /* server loop */

  free(srv);  

}


