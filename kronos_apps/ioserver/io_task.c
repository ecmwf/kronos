
#include "ioserver/io_task.h"

#include "ioserver/io_task_read.h"
#include "ioserver/io_task_read_nv.h"
#include "ioserver/io_task_write.h"
#include "ioserver/io_task_write_nv.h"

#include "common/json.h"
#include "common/logger.h"

#include <stdio.h>
#include <string.h>

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <assert.h>
#include "limits.h"



/*
 * IO task factory from json
 */
IOTaskFunctor* iotask_factory_from_json(const JSON* config, void* msg_data){

    const char* iotask_name;
    json_as_string_ptr(json_object_get(config, "name"), &iotask_name);

    DEBG2("got iotask of name: %s", iotask_name);

    if (!strcmp(iotask_name, "writer")){
        return init_iotask_write(config, msg_data);

    } else if (!strcmp(iotask_name, "nvram_writer")){
        return init_iotask_write_nvram(config, msg_data);

    } else if (!strcmp(iotask_name, "reader")){
        return init_iotask_read(config);

    } else if (!strcmp(iotask_name, "nvram_reader")){
        return init_iotask_read_nvram(config);
    }

    ERRO2("IO task %s not recognised!", iotask_name);

    return NULL;
}



/*
 * IO task factory from msg
 */
IOTaskFunctor* iotask_factory_from_msg(NetMessage* msg){

    JSON* config_json;
    IOTaskFunctor* funct;
    void* msg_data;

    config_json = json_from_string(msg->head);
    msg_data = (void*)msg->payload;

    funct = iotask_factory_from_json(config_json, msg_data);

    if (funct == NULL){
        ERRO1("IO task not recognised!");
    }

    return funct;

}
/* ================== IO_TASK FACTORY (END) ================== */
