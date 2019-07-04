
#include "common/json.h"
#include "common/logger.h"
#include "common/network/network.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>


int main(int argc, char **argv) {

    const char *server_host;
    int server_port;

    /* server connection */
    NetConnection* srv_conn;
    NetMessage* msg_ack;

    /* check the command line arguments.. */
    if (argc < 3) {

        INFO1("ERROR: Invalid arguments passed\n");
        INFO1("Usage: \n");
        INFO1("kill_worker <server-host> <server-port>\n");
        ERRO();

    } else {

        server_host = argv[1];
        server_port = atoi(argv[2]);

        INFO2("server_host: %s\n", server_host);
        INFO2("server_port: %i\n", server_port);
    }

    /* server host/port setup  */
    INFO3("H: %s, P: %i\n", server_host, server_port);

    /* server host/port setup */
    srv_conn = connect_to_server(server_host, server_port);
    if(srv_conn == NULL){
        FATL1("Connection error!");
    }

    /* send the message line to the server and wait for the reply */
    if (terminate_server(srv_conn)){
        ERRO1("Message send failed!");
    };

    /* wait for server acknowledgment */
    msg_ack = recv_net_msg(srv_conn);
    DEBG2("Acknowledgment from server: %s", msg_ack->head);

    /* close the connection */
    close_connection(srv_conn);

    INFO1("server termination request sent correctly.\n");
    return 0;
}


