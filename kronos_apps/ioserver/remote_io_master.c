
#include "common/json.h"
#include "common/logger.h"
#include "common/network/network.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <unistd.h>


int main(int argc, char **argv) {

    /* connection */
    FILE* hostfile_ptr;
    const char* host_file;
    JSON* json_hosts;
    int hosts_len;
    int ihost;    
    long int _msg_host;

    const char** hosts;
    long int* ports;

    /* json parsing/msg */
    const char* input_json_str;
    const JSON* json_input;
    const JSON* _jsonmsg;

    char jsonbuf[JSON_MSG_BUFFER];
    char reply_buf[JSON_MSG_BUFFER];
    int imsg, msg_size, io_msg_len;

    /* net message */
    NetMessage* msg;
    int head_l=10;
    char* head="ciaociao9";
    int pay_l=5;
    char* pay="12345";

    /* server connection */
    NetConnection* srv_conn;

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
    INFO2("opening file %s..", host_file);
    hostfile_ptr = fopen(host_file, "r");
    if (hostfile_ptr != NULL) {
        json_hosts = parse_json(hostfile_ptr);
        INFO2("file %s opened", host_file);
    } else {
        ERRO2("opening file %s failed!", host_file);
    }
    hosts_len = json_array_length(json_hosts);
    INFO2("hosts_len %i", hosts_len);

    hosts= malloc(hosts_len * sizeof(char *));
    ports= malloc(hosts_len * sizeof(long int));
    for (ihost=0; ihost<hosts_len; ihost++){

        /* make populate host/port arrays (NB: memory responsiility is still with JSON obj) */
        if (json_as_string_ptr(json_object_get(json_array_element(json_hosts, ihost), "host"), &hosts[ihost]) < 0){
            ERRO1("host not read correctly!");
        };

        if (json_object_get_integer(json_array_element(json_hosts, ihost), "port", &ports[ihost]) < 0){
            ERRO1("port not read correctly!");
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
        msg_size = write_json_string(jsonbuf, JSON_MSG_BUFFER, _jsonmsg);
        DEBG3("json message size %i: %s", msg_size, jsonbuf);

        /* server host/port setup */
        srv_conn = connect_to_server(hosts[_msg_host], ports[_msg_host]);



        /* pack a msg */
        msg = pack_net_message(&head_l, head, &pay_l, pay);
        send_net_msg(srv_conn, msg);


#if 0
        /* send the message line to the server and wait for the reply */
        if (send_msg(srv_conn, jsonbuf, msg_size)){
            ERRO1("message send, failed!");
        };

        /* print the server's reply */
        if (recv_msg(srv_conn, reply_buf, JSON_MSG_BUFFER)) {
            ERRO1("message send, failed!");
        }

        INFO2("Acknowledgement from server: %s", reply_buf);
        DEBG2("strlen(reply_buf): %i", strlen(reply_buf));
#endif

        /* close the connection */
        close_connection(srv_conn);
    }

    /* free remaining json ptrs */
    free_json(json_hosts);
    json_hosts = NULL;

    free_json((JSON*)json_input);
    json_input = NULL;
    _jsonmsg = NULL;

    /* free host/port top ptrs */
    free(hosts);
    free(ports);

    INFO1("Master finished successfully.");

}
