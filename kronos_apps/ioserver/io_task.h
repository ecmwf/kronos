#ifndef io_task_H
#define io_task_H

#include "common/json.h"

#define MAX_WRITE_SIZE 1048576



/* generic functor for an iotask */
typedef struct IOTaskFunctor {
    void* data;
    int (*execute)(const void* data);
} IOTaskFunctor;


/* data for iotask write */
typedef struct IOTaskWriteData {

    const char* type;
    const char* file;
    const char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;

} IOTaskWriteData;


/* data for iotask write on nvram*/
typedef struct IOTaskWriteDataNVRAM {

    const char* type;
    const char* file;
    const char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;
    long int pool_size;

} IOTaskWriteDataNVRAM;


/* data for iotask read */
typedef struct IOTaskReadData {

    const char* type;
    const char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

} IOTaskReadData;


/* data for iotask read (from NVRAM) */
typedef struct IOTaskReadDataNVRAM {

    const char* type;
    const char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

} IOTaskReadDataNVRAM;



/* iotask functor factory (from JSON) */
IOTaskFunctor* iotask_factory_from_json(const JSON* config);

/* iotask functor factory (from string) */
IOTaskFunctor* iotask_factory_from_string(const char* config);


#endif
