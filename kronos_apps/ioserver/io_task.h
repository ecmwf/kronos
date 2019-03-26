#ifndef io_task_H
#define io_task_H

#include "common/json.h"
#include "common/network/message.h"

#define MAX_WRITE_SIZE 1048576



/* sized data */
typedef struct SizedData {
    long int size;
    void* content;
} SizedData;


/* generic functor for an iotask */
typedef struct IOTaskFunctor {

    /* input-data needed by this task */
    void* data;

    /*
     * execute the task and return output data (if any)
     * NB: caller has ownership of output data
    */
    int (*execute)(void* data);

    int (*free_data)(void* data);

    /* get name of the task */
    const char* (*get_name)(const void* data);

} IOTaskFunctor;


/* data for iotask write */
typedef struct IOTaskWriteData {

    char* name;
    char* file;
    char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;

    /* actual data to write */
    SizedData* data_to_write;

} IOTaskWriteData;


/* data for iotask write on nvram*/
typedef struct IOTaskWriteDataNVRAM {

    char* name;
    char* file;
    char* mode;

    long int n_bytes;
    long int offset;
    long int n_writes;
    long int pool_size;

    /* actual data to write */
    SizedData* data_to_write;

} IOTaskWriteDataNVRAM;


/* data for iotask read */
typedef struct IOTaskReadData {

    char* name;
    char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

    /* filled upon execution */
    SizedData* data_read;

} IOTaskReadData;


/* data for iotask read (from NVRAM) */
typedef struct IOTaskReadDataNVRAM {

    char* name;
    char* file;

    long int n_bytes;
    long int offset;
    long int n_reads;

    /* filled upon execution */
    SizedData* data_read;

} IOTaskReadDataNVRAM;



/* iotask functor factory (from JSON) */
IOTaskFunctor* iotask_factory_from_json(const JSON* config, void* msg_data);

/* iotask functor factory (from string) */
IOTaskFunctor* iotask_factory_from_msg(NetMessage* msg);


/* free sized data */
void free_sized_data(SizedData* data);


#endif
