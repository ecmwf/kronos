#ifndef IODATA_H
#define IODATA_H


#include <stdlib.h>

/* sized data */
typedef struct IOData {
    long int size;
    void* content;
} IOData;



/* create_io_data */
IOData* create_iodata(long int size);


/* check iodata for consistency */
int check_iodata(IOData* iodata);


/* free sized data */
void free_iodata(IOData* data);


#endif
