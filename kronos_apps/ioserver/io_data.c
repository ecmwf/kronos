
#include "ioserver/io_data.h"


/* free sized data */
void free_iodata(IOData* data){
    free(data->content);
    free(data);
}
