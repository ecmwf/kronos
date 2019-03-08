#ifndef network_H
#define network_H

#include "common/bool.h"
#include "limits.h"


/* max buffer allowed for msg's */
#define MSG_BUFFER 1024

/* max buffer allowed for json msg's */
#define JSON_MSG_BUFFER 1024*1024


/* some info of a connection */
typedef struct ServerConnection {

    char host[PATH_MAX];
    long int port;
    int socket_fd;
    bool isConnectionOpen;

} ServerConnection;


/* connect to a server by host/port */
ServerConnection* connect_to_server(const char *host, const long int port);


/* send message (requires open connection) */
int send_msg(const ServerConnection* conn,
             const char* buffer,
             const int buffer_len);


/* recv message (requires open connection) */
int recv_msg(const ServerConnection* conn,
             char* buffer,
             const int buffer_len);


/* close a connection */
int close_connection(ServerConnection* conn);













#endif
