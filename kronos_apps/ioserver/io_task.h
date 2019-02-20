#ifndef io_task_H
#define io_task_H

#include "common/json.h"

#define MAX_WRITE_SIZE 1048576


/* an IO task (defines a file read or write operation)*/
typedef struct IOTask {

    char* type;
    char* file;
    char* write_mode;

    long int n_bytes;
    long int offset;
    long int n_writes;
    long int n_reads;

} IOTask;


/* get IO task properties */
const char* get_iotask_type(IOTask* iotask);
const char* get_iotask_file(IOTask* iotask);
const char* get_iotask_mode(IOTask* iotask);

long int get_iotask_bytes(IOTask* iotask);
long int get_iotask_nwrites(IOTask* iotask);
long int get_iotask_nreads(IOTask* iotask);
long int get_iotask_offset(IOTask* iotask);


/* An I/O task from a (correctly formed) JSON */
IOTask* iotask_from_json(const JSON* json_in);


/* An I/O task from a (correctly formed) stringified JSON */
IOTask* iotask_from_json_string(const char* _taskstr);



#endif
