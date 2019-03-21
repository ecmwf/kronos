
#include "network.h"
#include "common/logger.h"

#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "string.h"
#include "strings.h"






/*============== SERVER SIDE =============== */

/*
 * connect to a server and returns the socket descriptor
 */
static int get_socket(){

    int sockfd;
    int optval;

    /* socket: create the socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        ERRO1("ERROR opening socket");


    /* setsockopt: Handy debugging trick that lets
     * us rerun the server immediately after we kill it;
     * otherwise we have to wait about 20 secs.
     * Eliminates "ERROR on binding: Address already in use" error.
     */
    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR,
               (const void *)&optval , sizeof(int));


    return sockfd;
}


/* bind */
static int net_bind(Server* srv){

    /*
     * bind: associate the parent socket with a port
     */
    if (bind(srv->socket_fd,
             (struct sockaddr *) &(srv->_serveraddr),
             (socklen_t)sizeof(srv->_serveraddr)) < 0) {
      ERRO1("ERROR on binding");
    }

}


Server* create_server(const int* portno){

    Server* srv;

    struct sockaddr_in serveraddr;
    int socket_fd;


    DEBG2("asking for port %i", *portno);


    /* make space for srv */
    srv = malloc(sizeof(Server));

    /* get a socket fd*/
    socket_fd = get_socket();


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
           *portno);


    /* this is the port we will listen on */
    serveraddr.sin_port = htons((int)*portno);


    /* fill up server */
    strcpy(srv->host, "localhost");
    srv->port = *portno;
    srv->socket_fd = socket_fd;
    srv->is_binded = false;
    srv->is_listening = false;
    srv->_serveraddr = serveraddr;

    /* bind server to port*/
    net_bind(srv);

    return srv;

}



/* listen */
int net_listen(Server* srv){

    /*
     * listen: make this socket ready to accept connection requests
     */
    if (listen(srv->socket_fd, MAXNREQ) < 0)
      ERRO1("ERROR on listen");

}

/* accept */
int net_accept(Server* srv){

    socklen_t clientlen; /* byte size of client's address */
    struct sockaddr_in clientaddr; /* client addr */
    int childfd; /* child socket */
    struct hostent *hostp; /* client host info */
    char *hostaddrp;

    clientlen = sizeof(clientaddr);

    /*
     * accept: wait for a connection request
     */
    DEBG1("waiting for connections..");
    childfd = accept(srv->socket_fd,
                     (struct sockaddr *)&clientaddr,
                     &clientlen);

    if (childfd < 0) {
      ERRO2("ERROR on getting client connection (fd = %i", childfd);
      return -1;
    }

    DEBG2("connections %i accepted!", childfd);

    /*
     * gethostbyaddr: determine who sent the message
     */
    hostp = gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr,
                          sizeof(clientaddr.sin_addr.s_addr), AF_INET);
    if (hostp == NULL) {
      ERRO1("ERROR on gethostbyaddr");
      return -1;
    }

    hostaddrp = inet_ntoa(clientaddr.sin_addr);
    if (hostaddrp == NULL) {
      ERRO1("ERROR on inet_ntoa(clientaddr.sin_addr)");
      return -1;
    }

    INFO3("server established connection with %s (%s)", hostp->h_name, hostaddrp);
    return childfd;

}


/* broadcast a message to all clients */
int net_broadcast(Server* srv, NetMessage* msg){
    FATL1("not yet implemented!");
}


/* check if a msg is termination message */
bool check_termination_msg(NetMessage* msg){

    DEBG1("comparing msg VS termination-msg..");
    if (!strcmp(msg->head, SERVER_TERMINATION_STR)){
        return true;
    } else {
        return false;
    }
}


/* acknowledge reception with a pre-defined message */
int acknowledge_reception(NetConnection* conn){

    NetMessage* msg;
    int hd_len, no_len = 0;
    const char* head;

    head = SERVER_ACK_STR;
    hd_len = strlen(head)+1;

    DEBG2("hd_len: %i", hd_len);

    msg = create_net_message(&hd_len, SERVER_ACK_STR, &no_len, NULL);
    send_net_msg(conn, msg);

    free(msg);

}





/*
 * connect to a server by host/port
 */
NetConnection* connect_to_server(const char *host, const long int port){

    struct sockaddr_in serveraddr;
    struct hostent* server;

    /* fill up a connection */
    NetConnection* srv_conn;

    INFO3("host: %s, port: %i", host, port);

    /* start filling up the connection struct */
    srv_conn = malloc(sizeof(NetConnection));
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
int send_msg(const NetConnection* conn, const char* buffer, const int buffer_len){

    if (write(conn->socket_fd, buffer, buffer_len) < 0) {
        ERRO2("Writing to socket %i failed!", conn->socket_fd);
        return -1;

    } else {

          return 0;
    }
}


/* send a net message (hd_len + hd + payload_len + payload) */
int send_net_msg(const NetConnection* conn, NetMessage* msg){


    /* write length of header */
    if (write(conn->socket_fd, &(msg->head_len), sizeof(int)) < 0) {
        ERRO2("Writing head_len to socket %i failed!", conn->socket_fd);
        return -1;
    } else {
        DEBG2("msg->head_len: %i", msg->head_len);
    }


    /* if there is an header to write, proceed..) */
    if (msg->head_len > 0) {
        if (write(conn->socket_fd, msg->head, msg->head_len) < 0) {
            ERRO2("Writing head_len to socket %i failed!", conn->socket_fd);
            return -1;
        } else {
            DEBG2("msg->head: %s", msg->head);
        }
    }


    /* write payload length */
    if (write(conn->socket_fd, &(msg->payload_len), sizeof(int)) < 0) {
        ERRO2("Writing payload_len to socket %i failed!", conn->socket_fd);
        return -1;
    } else {
        DEBG2("msg->payload_len: %i", msg->payload_len);
    }


    /* if there is a payload to write, proceed..) */
    if (msg->payload_len > 0) {
        if (write(conn->socket_fd, msg->payload, msg->payload_len) < 0) {
            ERRO2("Writing payload to socket %i failed!", conn->socket_fd);
            return -1;
        } else {
            DEBG2("msg->payload: %s", msg->payload);
        }
    }


    return 0;
}


/* recv message (requires open connection) */
int recv_msg(const NetConnection* conn, char* buffer, int buffer_len){

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


/* recv a net message (hd_len + hd + payload_len + payload) */
NetMessage* recv_net_msg(const NetConnection* conn){

    /* make minimal space for incoming message */
    NetMessage* msg = malloc(sizeof(NetMessage));


    /* check that the connection is set as open.. */
    if (!conn->isConnectionOpen){
        ERRO2("Connection to host %s closed, read failed!", conn->host);
        return NULL;
    }


    /* read length of header */
    if (read(conn->socket_fd, &(msg->head_len), sizeof(int)) < 0) {
        ERRO2("Read head_len to socket %i failed!", conn->socket_fd);
        return NULL;
    } else {
        DEBG2("msg->head_len: %i", msg->head_len);
        msg->head = malloc(msg->head_len);
    }


    /* read header */
    if (msg->head_len > 0) {
        if (read(conn->socket_fd, msg->head, msg->head_len) < 0) {
            ERRO2("Read head_len to socket %i failed!", conn->socket_fd);
            return NULL;
        } else {
            DEBG2("msg->head: %s", (char*)msg->head);
        }
    }


    /* read length of payload */
    if (read(conn->socket_fd, &(msg->payload_len), sizeof(int)) < 0) {
        ERRO2("Read payload_len to socket %i failed!", conn->socket_fd);
        return NULL;
    } else {
        DEBG2("msg->payload_len: %i", msg->payload_len);
        msg->payload = malloc(msg->payload_len);
    }


    /* read payload */
    if (msg->payload_len > 0) {
        if (read(conn->socket_fd, msg->payload, msg->payload_len) < 0) {
            ERRO2("Read payload_len to socket %i failed!", conn->socket_fd);
            return NULL;
        } else {
            DEBG2("msg->payload: %s", msg->payload);
        }
    }

    return msg;
}


/* close a connection */
int close_connection(NetConnection* conn){

    close(conn->socket_fd);

}
