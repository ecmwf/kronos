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
NetMessage* pack_net_message(int* head_len,
                             char* head,
                             int* payload_len,
                             void* payload);

/* unpack a message */
int unpack_net_message(NetMessage*msg,
                       int* head_len,
                       char* head,
                       int* payload_len,
                       void* payload);


void free_net_message(NetMessage* msg);

#endif
