
#include "ioserver/io_data.h"
#include "common/logger.h"

#include "string.h"
#include <assert.h>
#include "limits.h"



/* allocates data to write io-tasks */
static void* create_content(long int nbytes){

    char* content;
    long int i;


    /* allocate nbytes in the content */
    content = (char*)malloc(nbytes+1);
    if (content == NULL){
        FATL2("Failed to allcate %i bytes of memory!", nbytes);
        return NULL;
    }

    /* set a string nbytes+1 long (incl last '\0') */
    for (i=0; i<nbytes; i++){
        *((char*)content+i) = (char)(i%10)+'0';
    }
    /*memset((void*)content, 'w', (int)nbytes-1);*/
    *((char*)content+nbytes) = '\0';

    DEBG2("*********** allocated %s: ", (char*)content);

    return (void*)content;
}



/* create_io_data */
IOData* create_iodata(long int size){

    IOData* iodata;

    if ((iodata = malloc(sizeof(IOData))) == NULL){
        ERRO1("data allocation failed!");
        return NULL;
    }

    /* set the sizes of content */
    iodata->size = size;
    iodata->size_as_string = size+1;

    if ((iodata->content = create_content(size)) == NULL){
        ERRO1("data creation failed!");
        return NULL;
    } else {
        return iodata;
    }

}


/* check iodata for consistency */
int check_iodata(IOData* iodata){

    char* expected;

    DEBG1("checking iodata..");

    expected = (char *)create_content(iodata->size);
    if (expected == NULL){
        ERRO1("iodata creation failed!");
        return 1;
    }

    if(!strcmp((char*)iodata->content, (char*)expected)){

        DEBG1("iodata OK");
        free(expected);
        return 0;

    } else {

        ERRO1("iodata NOT OK!");
        DEBG2("expected: %s", (char*)expected);
        DEBG2("iodata content: %s", (char*)iodata->content);
        free(expected);
        return 1;

    }

}


/* free sized data */
void free_iodata(IOData* data){
    free(data->content);
    free(data);
}



/* print subcontent */
int print_iodata_content(IOData* data,
                         long int idx_from,
                         long int idx_to){

    assert(idx_from > 0);
    assert(idx_to < data->size-1);
    assert(idx_from < idx_to);

    /* it actually prints only if in debug mode.. */
    DEBG3("data: %.*s", idx_to-idx_from, (char*)(data->content)+idx_from);

    return 0;

}












