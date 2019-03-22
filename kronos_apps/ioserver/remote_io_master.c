
#include "common/json.h"
#include "common/logger.h"
#include "common/network/network.h"

#include "io_task.h"

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
    int imsg, msg_size, io_msg_len;

    /* server connection */
    NetConnection* srv_conn;

    /* net message */
    NetMessage* msg;
    NetMessage* msg_ack;
    int null_payl = 0;

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

        /* check that the host ID is within the range of known hosts */
        if (_msg_host >= hosts_len) {
            FATL3("Message for host \"%i\" (0-based list) but there is/are only %i hosts defined!",
                  _msg_host,
                  hosts_len);
        }

        /* server host/port setup */
        DEBG3("asking connection to %s: %i", hosts[_msg_host], ports[_msg_host]);
        srv_conn = connect_to_server(hosts[_msg_host], ports[_msg_host]);
        if(srv_conn == NULL){
            FATL1("Connection error!");
        }


        /* pack-send-free a msg */
        DEBG1("sending message..");
        msg = create_net_message(&msg_size, jsonbuf, &null_payl, NULL);
        send_net_msg(srv_conn, msg);
        free(msg);
        DEBG1("message sent!");

        /* wait for server acknowledgment */
        msg_ack = recv_net_msg(srv_conn);
        DEBG2("Acknowledgment from server: %s", msg_ack->head);


        /* close the connection */
        close_connection(srv_conn);
        DEBG1("connection closed!");

        /* free connection */
        free(srv_conn);
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
