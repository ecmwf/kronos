#ifndef IODATA_H
#define IODATA_H


#include <stdlib.h>

/* sized data */
typedef struct IOData {

    /* size of content */
    long int size;

    /* content */
    void* content;

} IOData;



/* create_io_data */
IOData* create_iodata(long int size);


/*
 * fill IOdata from input buffer
 * NB: the content is still expected
 * to be coherent (0123456...)
 */
IOData* fill_iodata(long int content_size, void* content);


/* check iodata for consistency */
int check_iodata(IOData* iodata);


/* free sized data */
void free_iodata(IOData* data);


/* print subcontent */
int print_iodata(IOData* data,
                 long int idx_from,
                 long int idx_to);

#endif
