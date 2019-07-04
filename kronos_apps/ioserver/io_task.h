#ifndef io_task_H
#define io_task_H

#include "common/json.h"
#include "common/network/message.h"
#include "ioserver/io_data.h"

#define MAX_WRITE_SIZE 1048576


/*
 * generic functor for an iotask
 */
typedef struct IOTaskFunctor {

    /* input-data needed by this task */
    void* data;

    /*
     * execute the task and return output data (if any)
    */
    int (*execute)(void* data, IOData** out_data);

    int (*free_data)(void* data);

    /* get name of the task */
    const char* (*get_name)(const void* data);

} IOTaskFunctor;



/*
 * iotask functor factory (from JSON)
 */
IOTaskFunctor* iotask_factory_from_json(const JSON* config, void* msg_data);



/*
 * iotask functor factory (from string)
 */
IOTaskFunctor* iotask_factory_from_msg(NetMessage* msg);


#endif
