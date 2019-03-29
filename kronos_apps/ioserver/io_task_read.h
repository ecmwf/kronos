#ifndef io_task_READ_H
#define io_task_READ_H

#include "ioserver/io_task.h"
#include "ioserver/io_data.h"

/* data for iotask read */
typedef struct IOTaskReadInputData {

    char* name;
    char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

} IOTaskReadInputData;


/* init functor */
IOTaskFunctor* init_iotask_read(const JSON* config);


#endif
