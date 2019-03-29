#ifndef io_task_WRITE_H
#define io_task_WRITE_H

#include "ioserver/io_task.h"
#include "ioserver/io_data.h"
#include "common/json.h"


/*
 * data for iotask write
 */
typedef struct IOTaskWriteInputData {

    char* name;
    char* file;
    char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;

    /* actual data to write */
    IOData* data_to_write;

} IOTaskWriteInputData;


/*
 * init functor
 */
IOTaskFunctor* init_iotask_write(const JSON* config, void* msg_data);


#endif
