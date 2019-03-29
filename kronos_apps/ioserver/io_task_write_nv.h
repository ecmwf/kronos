#ifndef io_task_WRITE_NV_H
#define io_task_WRITE_NV_H

#include "ioserver/io_task.h"
#include "ioserver/io_data.h"
#include "common/json.h"


/* data for iotask write on nvram*/
typedef struct IOTaskWriteNVRAMInputData {

    char* name;
    char* file;
    char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;
    long int pool_size;

    /* actual data to write */
    IOData* data_to_write;

} IOTaskWriteNVRAMInputData;


/* init functor */
IOTaskFunctor* init_iotask_write_nvram(const JSON* config);

#endif
