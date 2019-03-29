#ifndef IODATA_H
#define IODATA_H


#include <stdlib.h>

/* sized data */
typedef struct IOData {

    /* size of content */
    long int size;

    /* size of content as string (= size+1) */
    long int size_as_string;

    /* content */
    void* content;

} IOData;



/* create_io_data */
IOData* create_iodata(long int size);


/* check iodata for consistency */
int check_iodata(IOData* iodata);


/* free sized data */
void free_iodata(IOData* data);


/* print subcontent */
int print_iodata(IOData* data,
                 long int idx_from,
                 long int idx_to);

#endif
