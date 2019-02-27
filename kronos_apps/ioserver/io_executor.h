#ifndef io_executor_H
#define io_executor_H

#include "io_task.h"


/* Execute an I/O task (from an IOTask)*/
int execute_io_task(IOTask* iotask);

/* Execute an I/O task (from stringified JSON)*/
int execute_io_task_from_string(const char * io_json_str);


#endif
