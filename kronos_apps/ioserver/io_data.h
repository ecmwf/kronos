#ifndef IODATA_H
#define IODATA_H


#include <stdlib.h>

/* sized data */
typedef struct IOData {
    long int size;
    void* content;
} IOData;



/* free sized data */
void free_iodata(IOData* data);


#endif
