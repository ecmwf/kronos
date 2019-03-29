#ifndef io_task_READ_NV_H
#define io_task_READ_NV_H

#include "ioserver/io_task.h"
#include "ioserver/io_data.h"
#include "common/json.h"


/* data for iotask read (from NVRAM) */
typedef struct IOTaskReadNVRAMInputData {

    char* name;
    char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

} IOTaskReadNVRAMInputData;


/* init functor */
IOTaskFunctor* init_iotask_read_nvram(const JSON* config);


#endif
