
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


/*
 * execute the IO task and give output back if present.
 * E.g. if it's executing a "read" type of task
*/
int execute_io_task(NetMessage* msg, SizedData** output){

    /* IO task functor */
    IOTaskFunctor* iotaskfunct;
    IOTaskReadData* reader_data;
    /* IOTaskReadDataNVRAM* reader_data_nv; */

    /* if io_taks ends with an error */
    int err_iotask = 0;

    if (msg->head_len){
        DEBG2("asked to perform task: %s", msg->head);
        iotaskfunct = iotask_factory_from_msg(msg);
        err_iotask = iotaskfunct->execute(iotaskfunct->data);

        if (err_iotask){
            ERRO1("reported error in executing IO task!");
        }

        /* print the data back if it's a read task */
        if ( !strcmp(iotaskfunct->get_name(iotaskfunct->data), "reader")){
            reader_data = (IOTaskReadData*)(iotaskfunct->data);
            DEBG2("Read data: %s", (char*)(reader_data->data_read->content) );

/*            memcpy(*output->size, reader_data->data_read->size, sizeof(long int));
            *(output->size) = reader_data->data_read; */

        } else {

            *output = NULL;

        }

    } else {
        ERRO1("Empty message received!");
    }

    return err_iotask;
}

/*
 * handle an incoming connection and
 * executes the io_task as requested.
 */
int handle_connection(int conn){

    NetMessage* msg;
    NetConnection* conn_with_client;
    void* task_output;

    int null_size=0;
    char* null_head=NULL;

    /* preallocate client connection */
    conn_with_client = create_connection(conn);

    /* receive one message and acknowledge */
    msg = recv_net_msg(conn_with_client);
    DEBG1("message received!");

    acknowledge_reception(conn_with_client);
    DEBG1("acknowledgment sent!");

    /* honour a termination request (when arrives) */
    if (check_termination_msg(msg)){
        INFO1("received kill signal => terminating.");
        close(conn_with_client->socket_fd);
        return SRV_OFF_CODE;
    }

    /* perform the IO task as requested */
    if (execute_io_task(msg, &task_output) < 0){
      ERRO1("error executing the task!");
    }

    if (task_output != NULL){
        DEBG2("output data of execute_io_task: %s", (char*)task_output);
        /* pack the data and send it back.. */
        /* msg = create_net_message(&null_size, null_head, &writer_nbytes, task_output); */
        if (send_net_msg(conn, msg) < 0){
            ERRO1("error sending data back");
            return -1;
        }
        free(msg);

    } else {
        DEBG1("output data of execute_io_task is NULL");
    }

    free(task_output);

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


