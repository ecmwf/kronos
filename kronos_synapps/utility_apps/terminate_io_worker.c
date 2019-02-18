
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "kronos/json.h"
#include "logger.h"


/*
 * connect to a server and returns the socket descriptor
 */
int get_socket(){

    int sockfd;

    /* socket: create the socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        FATL1("ERROR opening socket");

    return sockfd;
}


int main(int argc, char **argv) {

    int sockfd, n, errno;
    struct sockaddr_in serveraddr;
    struct hostent *server;
    const char* server_host;
    int server_port;

    const char *kill_msg = "terminate-server";

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

    /* gethostbyname: get the server's DNS entry */
    server = gethostbyname(server_host);
    if (server == NULL) exit(0);

    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr_list[0], (char *)&serveraddr.sin_addr.s_addr, server->h_length);
    serveraddr.sin_port = htons(server_port);

    /* get a socket and connect it to the server address */
    sockfd = get_socket();
    errno = connect(sockfd, (const struct sockaddr *)&serveraddr, sizeof(serveraddr));
    if (errno < 0) {
        printf("error getting connecting the socket: %i\n", errno);
    }

    n = write(sockfd, kill_msg, strlen(kill_msg));
    if (n < 0) {
        ERRO1("ERROR writing to socket");
    }

    /* close the socket */
    close(sockfd);

    INFO1("server termination request sent correctly.\n");
    return 0;
}


