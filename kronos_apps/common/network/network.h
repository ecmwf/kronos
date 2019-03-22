#ifndef network_H
#define network_H

#include "common/bool.h"
#include "common/network/message.h"

#include "limits.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>


/* max buffer allowed for msg's */
#define MSG_BUFFER 1024

/* max buffer allowed for json msg's */
#define JSON_MSG_BUFFER 1024*1024

/* server termination string */
#define SERVER_TERMINATION_STR "terminate-server"

/* msg reception acknowledgment string */
#define SERVER_ACK_STR "message-received"

/* max number of queued requests for connections */
#define MAXNREQ 1024



/*============== SERVER SIDE =============== */

/* net protocol */
enum net_protocol {
    TCP,
    UDP
};


/* Connection TO/FROM server */
typedef struct NetConnection {

    char host[PATH_MAX];
    long int port;
    int socket_fd;
    bool isConnectionOpen;

} NetConnection;



/* a server */
typedef struct Server {

    char host[PATH_MAX];
    int port;
    int socket_fd;

    bool is_binded;
    bool is_listening;

    /* for internal use */
    struct sockaddr_in _serveraddr;

} Server;



/* setup a server */
Server* create_server(const int* portno);

/* listen */
int net_listen(Server* srv);

/* accept */
int net_accept(Server* srv);

/* broadcast a message to all clients */
int net_broadcast(Server* srv, NetMessage* msg);

/* check if a msg is termination message */
bool check_termination_msg(NetMessage* msg);

/* acknowledge_reception */
int acknowledge_reception(NetConnection* conn);



/*============== CLIENT SIDE =============== */

/* connect to a server by host/port */
NetConnection* connect_to_server(const char *host, const long int port);

/* take a server down */
int terminate_server(NetConnection* conn);



/*============== SHARED =============== */

/* close a connection */
int close_connection(NetConnection* conn);

/* send net_message (requires open connection) */
int send_net_msg(const NetConnection* conn, NetMessage* msg);

/* recv net_message (requires open connection) */
NetMessage* recv_net_msg(const NetConnection* conn);






#endif
