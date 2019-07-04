#ifndef message_H
#define message_H


/* a simple network message */
typedef struct NetMessage {

    int head_len;
    char* head;
    int payload_len;
    void* payload;

} NetMessage;


/* pack a message */
NetMessage* create_net_message(long int* head_len,
                               char* head,
                               long int* payload_len,
                               void* payload);

/* unpack a message */
int unpack_net_message(NetMessage*msg,
                       long int* head_len,
                       char* head,
                       long int* payload_len,
                       void* payload);


void free_net_message(NetMessage* msg);

#endif
