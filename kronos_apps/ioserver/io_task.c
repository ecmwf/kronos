
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>

#include "io_task.h"
#include "common/json.h"
#include "common/logger.h"



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


long int get_iotask_poolsize(IOTask* iotask){

    if (iotask->pool_size < 0){
        ERRO1("invalid #poolsize!");
        exit(1);
    } else {
        return iotask->pool_size;
    }

}


long int get_iotask_nreads(IOTask* iotask){

    if (iotask->n_reads < 0){
        ERRO1("invalid #reads!");
        exit(1);
    } else {
        return iotask->n_reads;
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


IOTask* iotask_from_json(const JSON* json_in){

    IOTask* iotask = malloc(sizeof(IOTask));

    const char* _io_task_type = NULL;
    const char* _io_task_file = NULL;
    const char* _io_task_mode = NULL;

    long int _io_task_bytes = -1;
    long int _io_task_nwrites = -1;
    long int _io_task_poolsize = -1;
    long int _io_task_nreads = -1;
    long int _io_task_offset = -1;

    DEBG2("=======> executing %s\n", __func__);


    json_as_string_ptr(json_object_get(json_in, "type"), &_io_task_type);
    iotask->type = malloc(strlen(_io_task_type)+1);
    strncpy(iotask->type, _io_task_type, strlen(_io_task_type)+1);
    DEBG2("IO type: %s", _io_task_type);


    json_object_get_integer(json_in, "bytes", &_io_task_bytes);
    iotask->n_bytes = _io_task_bytes;
    DEBG2("IO bytes: %li", _io_task_bytes);


    json_as_string_ptr(json_object_get(json_in, "file"), &_io_task_file);
    iotask->file = malloc(strlen(_io_task_file)+1);
    strncpy(iotask->file, _io_task_file, strlen(_io_task_file)+1);
    DEBG2("IO file: %s", _io_task_file);


    json_object_get_integer(json_in, "offset", &_io_task_offset);
    iotask->offset = _io_task_offset;
    DEBG2("IO offset: %li", _io_task_offset);


    /* n_writes only present for writing tasks */
    if (json_object_has(json_in, "write_mode")){

        json_object_get_integer(json_in, "n_writes", &_io_task_nwrites);
        iotask->n_writes = _io_task_nwrites;
        DEBG2("IO n writes: %li", _io_task_nwrites);

        json_as_string_ptr(json_object_get(json_in, "write_mode"), &_io_task_mode);
        iotask->write_mode = malloc(strlen(_io_task_mode)+1);
        strncpy(iotask->write_mode, _io_task_mode, strlen(_io_task_mode)+1);
        DEBG2("IO mode: %s", _io_task_mode);
    }

    /* only to be used if type is writer to NVRAM */
    if (!strcmp(_io_task_type, "nvram_writer")){
        json_object_get_integer(json_in, "pool_size_8mb", &_io_task_poolsize);
        iotask->pool_size = _io_task_poolsize;
        DEBG2("pool_size_8mb: %li", _io_task_poolsize);
    }

    /* n_reads only present for reading tasks */
    if (!strcmp(_io_task_type, "reader")){

        json_object_get_integer(json_in, "n_reads", &_io_task_nreads);
        iotask->n_reads = _io_task_nreads;
        DEBG2("IO n reads: %li", _io_task_nreads);
    }

    DEBG2("==> Finished executing %s\n", __func__);

    return iotask;

}



IOTask* iotask_from_json_string(const char* _taskstr){

    JSON* _json;
    IOTask* _iotask;
    _json = json_from_string(_taskstr);
    _iotask = iotask_from_json(_json);
    free(_json);

    return _iotask;
}


