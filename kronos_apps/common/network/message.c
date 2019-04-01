
#include "message.h"
#include "common/logger.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/* pack a message */
NetMessage* create_net_message(long int* head_len,
                               char* head,
                               long int* payload_len,
                               void* payload){

    NetMessage* msg = malloc(sizeof(NetMessage));

    DEBG1("creating message..");

    /* copy header */
    msg->head_len = *head_len;
    DEBG2("msg->head_len %i ", msg->head_len);
    if (msg->head_len){
        msg->head = (char*)malloc(*head_len+1);
        if (msg->head == NULL){
            ERRO1("memory allocation error!");
            return NULL;
        }
        strcpy(msg->head, head);
    } else {
        msg->head = NULL;
    }

    /* copy payload */
    msg->payload_len = *payload_len;
    DEBG2("msg->payload_len %i ", msg->payload_len);
    if (msg->payload_len){
        msg->payload = (void*)malloc(*payload_len+1);
        if (msg->payload == NULL){
            ERRO1("memory allocation error!");
            return NULL;
        }
        DEBG1("copying payload..");
        memcpy(msg->payload, payload, *payload_len);
        *((char*)(msg->payload)+msg->payload_len) = '\0';
        DEBG1("payload copied!");
    } else {
        msg->payload = NULL;
    }

    DEBG1("message created!");

    return msg;

}

/* unpack a message */
int unpack_net_message(NetMessage*msg,
                       long int* head_len,
                       char* head,
                       long int* payload_len,
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
