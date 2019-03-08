
#include "network.h"
#include "common/logger.h"

#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include "string.h"
#include "strings.h"


/*
 * connect to a server and returns the socket descriptor
 */
static int get_socket(){

    int sockfd;

    /* socket: create the socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        ERRO1("ERROR opening socket");

    return sockfd;
}


/*
 * connect to a server by host/port
 */
ServerConnection* connect_to_server(const char *host, const long int port){

    struct sockaddr_in serveraddr;
    struct hostent* server;

    /* fill up a connection */
    ServerConnection* srv_conn;

    INFO3("H: %s, P: %i", host, port);

    /* start filling up the connection struct */
    srv_conn = malloc(sizeof(ServerConnection));
    strcpy(srv_conn->host, host);
    srv_conn->port = port;

    /* gethostbyname: get the server's DNS entry */
    server = gethostbyname(host);
    if (server == NULL) {
        ERRO2("ERROR connecting to %s (internal error)", host);
        return NULL;
    }

    /* preparing the server internals.. */
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr_list[0], (char *)&serveraddr.sin_addr.s_addr, server->h_length);
    serveraddr.sin_port = htons(port);

    /* get socket */
    srv_conn->socket_fd = get_socket();

    if (connect(srv_conn->socket_fd, (const struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0){

      ERRO1("ERROR connecting");
      return NULL;

    } else {
        srv_conn->isConnectionOpen = true;
    }

    /* return the connection */
    return srv_conn;
}


/* send message (requires open connection) */
int send_msg(const ServerConnection* conn, const char* buffer, int* buffer_len){

    if (write(conn->socket_fd, buffer, *buffer_len) < 0) {
        ERRO2("Writing to socket %i failed!", conn->socket_fd);
        return -1;

    } else {

          return 0;
    }
}


/* recv message (requires open connection) */
int recv_msg(const ServerConnection* conn, char* buffer, int buffer_len){

    /* check that the connection is open.. */
    if (!conn->isConnectionOpen){
        ERRO2("Connection to host %s closed, read failed!", conn->host);
        return -1;
    }

    /* print the server's reply */
    if (read(conn->socket_fd, buffer, buffer_len) < 0) {
        ERRO2("Reading from socket %i failed!", conn->socket_fd);
        return -1;

    } else {

          return 0;
    }

}


/* close a connection */
int close_connection(ServerConnection* conn){

    close(conn->socket_fd);

}
