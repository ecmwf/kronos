
#include "ioserver/io_data.h"
#include "common/logger.h"

#include "string.h"



/* allocates data to write io-tasks */
static void* create_content(const int nbytes){

    char* content;

    /* allocate nbytes in the content */
    content = (char*)malloc(nbytes);
    if (content == NULL){
        FATL2("Failed to allcate %i bytes of memory!", nbytes);
        return NULL;
    }

    /* set a string nbytes long (incl last '\0') */
    memset((void*)content, 'w', (int)nbytes-1);
    *((char*)content+nbytes-1) = '\0';

    return (void*)content;
}



/* create_io_data */
IOData* create_iodata(long int size){

    IOData* iodata;

    if ((iodata = malloc(sizeof(IOData))) == NULL){
        ERRO1("data allocation failed!");
        return NULL;
    }

    iodata->size = size;
    if ((iodata->content = create_content(size)) == NULL){
        ERRO1("data creation failed!");
        return NULL;
    } else {
        return iodata;
    }

}


/* check iodata for consistency */
int check_iodata(IOData* iodata){

    ERRO1("not implemented!");
    return -1;

}


/* free sized data */
void free_iodata(IOData* data){
    free(data->content);
    free(data);
}
