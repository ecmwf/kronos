
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>

#include "io_task.h"
#include "kronos/json.h"
#include "logger.h"



const char* get_iotask_type(IOTask* iotask){

    if (iotask->type == NULL){
        ERRO1("message type invalid");

        exit(1);
    } else {
        return iotask->type;
    }
}

const char* get_iotask_file(IOTask* iotask){

    if (iotask->file == NULL){
        ERRO1("message file invalid!");
        exit(1);
    } else {
        return iotask->file;
    }
}

const char* get_iotask_mode(IOTask* iotask){

    if (iotask->write_mode == NULL){

        ERRO1("message mode invalid!");
        return NULL;

    } else if (get_iotask_mode(iotask) == "reader"){

        ERRO1("warning: message is reader!");
        return iotask->write_mode;

    } else {

        return iotask->write_mode;
    }
}

long int get_iotask_bytes(IOTask* iotask){

    if (iotask->n_bytes < 0){
        ERRO1("invalid #bytes!");
        exit(1);
    } else {
        return iotask->n_bytes;
    }
}

long int get_iotask_nwrites(IOTask* iotask){

    if (iotask->n_writes < 0){
        ERRO1("invalid #writes!");
        exit(1);
    } else {
        return iotask->n_writes;
    }
}

long int get_iotask_offset(IOTask* iotask){

    if (iotask->offset < 0){
        ERRO1("invalid offset!");
        exit(1);
    } else {
        return iotask->offset;
    }
}

void iotask_fromstring(IOTask* iotask, const char* _taskstr){

    JSON* _json;

    const char* _io_task_type = NULL;
    const char* _io_task_file = NULL;
    const char* _io_task_mode = NULL;

    long int _io_task_bytes = -1;
    long int _io_task_nwrites = -1;
    long int _io_task_nreads = -1;
    long int _io_task_offset = -1;


    DEBG2("=======> executing %s\n", __func__);

    _json = json_from_string(_taskstr);

    json_as_string_ptr(json_object_get(_json, "type"), &_io_task_type);
    DEBG2("IO type: %s\n", _io_task_type);

    json_object_get_integer(_json, "bytes", &_io_task_bytes);
    DEBG2("IO bytes: %li\n", _io_task_bytes);

    json_as_string_ptr(json_object_get(_json, "file"), &_io_task_file);
    DEBG2("IO file: %s\n", _io_task_file);

    json_object_get_integer(_json, "offset", &_io_task_offset);
    DEBG2("IO offset: %li\n", _io_task_offset);

    /* n_writes only present for writing tasks */
    if (json_object_has(_json, "write_mode")){

        json_object_get_integer(_json, "n_writes", &_io_task_nwrites);
        DEBG2("IO n writes: %li\n", _io_task_nwrites);

        json_as_string_ptr(json_object_get(_json, "write_mode"), &_io_task_mode);
        DEBG2("IO mode: %s\n", _io_task_mode);
    }

    /* n_reads only present for reading tasks */
    if (!strcmp(_io_task_type, "reader")){
        json_object_get_integer(_json, "n_reads", &_io_task_nreads);
        DEBG2("IO n reads: %li\n", _io_task_nreads);
    }

    /* pack the IOTask */
    iotask->type = _io_task_type;
    iotask->file = _io_task_file;
    iotask->n_bytes = _io_task_bytes;
    iotask->offset = _io_task_offset;
    iotask->n_writes = _io_task_nwrites;
    iotask->write_mode = _io_task_mode;

    DEBG2("==> Finished executing %s\n", __func__);

}
