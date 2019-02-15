
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "kronos/json.h"
#include "logger.h"

#define JSON_BUF_LEN 1024
#define BUFSIZE JSON_BUF_LEN


/* 
 * error - wrapper for perror
 */
void error(char *msg) {
    perror(msg);
    exit(0);
}

/*
 * connect to a server and returns the socket descriptor
 */
int get_socket(){

    int sockfd;

    /* socket: create the socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        ERRO1("ERROR opening socket");

    return sockfd;
}


int main(int argc, char **argv) {

    /* connection */
    FILE* hostfile_ptr;
    const char* host_file;
    JSON* json_hosts;
    int hosts_len;
    int ihost;
    const JSON* _tmp_json;
    long int _msg_host;

    const char** hosts;
    long int* ports;

    int sockfd, n;
    struct sockaddr_in serveraddr;
    struct hostent* server;

    /* json parsing/msg */
    const char* input_json_str;
    const JSON* _jsonmsg;
    const JSON* json_input;

    char jsonbuf[JSON_BUF_LEN];
    char reply_buf[BUFSIZE];
    int io_msg_len;
    int msg_size;
    int imsg;


    /* check the command line arguments.. */
    if (argc < 3) {

        WARN1("ERROR: Invalid arguments passed");
        WARN1("USAGE:");
        WARN1("remote_io_master <host-file> <input-json-string>");
        ERRO();

    } else {

        host_file = argv[1];
        input_json_str = argv[2];
    }

    /* read the host_file and populate the hosts and ports arrays */
    hostfile_ptr = fopen(host_file, "r");
    if (hostfile_ptr != NULL) {
        json_hosts = parse_json(hostfile_ptr);
    }
    hosts_len = json_array_length(json_hosts);
    ports=(long int *) malloc(sizeof(long int) * hosts_len);
    hosts= malloc(hosts_len * sizeof(char *));

    for (ihost=0; ihost<hosts_len; ihost++){

        _tmp_json = json_array_element(json_hosts, ihost);
        json_as_string_ptr(json_object_get(_tmp_json, "host"), &hosts[ihost]);

        if (json_object_get_integer(_tmp_json, "port", &ports[ihost]) < 0){
            ERRO1("port not read correctly");
        };

    }

    /* generate the json from the input string */
    json_input = json_from_string(input_json_str);
    io_msg_len = json_array_length(json_input);

    DEBG2("number of IO messages: %i", io_msg_len);

    /* now process each IO task in sequence.. */
    for(imsg=0; imsg<io_msg_len; imsg++){

        /* reading json io message one at a time.. */
        _jsonmsg = json_array_element(json_input, imsg);
        json_object_get_integer(_jsonmsg, "host", &_msg_host);
        msg_size = write_json_string(jsonbuf, JSON_BUF_LEN, _jsonmsg);
        DEBG3("json message size %i: %s", msg_size, jsonbuf);

        /* =========== server host/port setup ================== */
        INFO3("H: %s, P: %i", hosts[_msg_host], ports[_msg_host]);

        /* gethostbyname: get the server's DNS entry */
        server = gethostbyname(hosts[_msg_host]);
        if (server == NULL)
          exit(0);

        bzero((char *) &serveraddr, sizeof(serveraddr));
        serveraddr.sin_family = AF_INET;
        bcopy((char *)server->h_addr_list[0], (char *)&serveraddr.sin_addr.s_addr, server->h_length);
        serveraddr.sin_port = htons(ports[_msg_host]);
        /* =============================================================== */

        sockfd = get_socket();
        if (connect(sockfd, (const struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0)
          ERRO1("ERROR connecting");

        /* send the message line to the server and wait for the reply */
        n = write(sockfd, jsonbuf, msg_size);
        if (n < 0)
          ERRO1("ERROR writing to socket");

        /* print the server's reply */
        n = read(sockfd, reply_buf, BUFSIZE);
        if (n < 0)
          ERRO1("ERROR reading from socket");

        INFO2("Acknowledgement from server: %s", reply_buf);
        DEBG2("strlen(reply_buf): %i", strlen(reply_buf));

        /* close the socket */
        close(sockfd);
    }

    free(ports);
    for (ihost=0; ihost<hosts_len; ihost++){
        free((char*)hosts[ihost]);
    }
    free(hosts);

    return 0;
}
