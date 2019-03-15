
#include "message.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/* pack a message */
NetMessage* create_net_message(int* head_len,
                             char* head,
                             int* payload_len,
                             void* payload){

    NetMessage* msg = malloc(sizeof(NetMessage));

    /* copy header */
    msg->head_len = *head_len;
    msg->head = (char*)malloc((*head_len+1) * sizeof(char));
    strcpy(msg->head, head);

    /* copy payload */
    msg->payload_len = *payload_len;
    msg->payload = (void*)malloc(*payload_len * sizeof(char));
    memcpy(msg->payload, payload, *payload_len);

    return msg;

}

/* unpack a message */
int unpack_net_message(NetMessage*msg,
                       int* head_len,
                       char* head,
                       int* payload_len,
                       void* payload){

    /* copy header */
    *head_len = msg->head_len;
    head = (char*)malloc((*head_len+1) * sizeof(char));
    strcpy(head, msg->head);

    /* copy payload */
    *payload_len = msg->payload_len;
    payload = (void*)malloc(*payload_len * sizeof(char));
    memcpy(payload, msg->payload, *payload_len);


    return 0;

}


void free_net_message(NetMessage* msg){

    free(msg->head);
    free(msg->payload);
    free(msg);
}
