
#include "ioserver/io_task_read.h"
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




static const char* get_name_taskr(const void* data){
    const IOTaskReadInputData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}



/* open-read-close */
static int execute_read_file(void* data, IOData** out_data){

    IOTaskReadInputData* task_data = data;

    FILE* file_d;
    long result;
    int ret;
    long int iread;    
    long int read_chunk = task_data->n_bytes / task_data->n_reads;

    DEBG1("--> executing reading.. ");

    /* allocate space for the output */
    DEBG2("allocating %i bytes", task_data->n_reads * read_chunk);
    *out_data = malloc(task_data->n_reads * read_chunk + 1);
    DEBG2("%i bytes allocated", task_data->n_reads * read_chunk);

    /* open-read-close */
    for (iread=0; iread<task_data->n_reads; iread++)
    {
        file_d = fopen(task_data->file, "rb");
        if (file_d != NULL) {

            ret = fseek(file_d, task_data->offset, SEEK_SET);

            if (ret == -1) {
                ERRO2("A error occurred seeking in file: %s (%d) %s", task_data->file);
                return -1;
            } else {

                /* read from file */
                result = fread( (char*)(*out_data) + task_data->offset,
                                1,
                                read_chunk,
                                file_d);

                if (result != read_chunk) {
                    ERRO2("A read error occurred reading file: %s (%d) %s", task_data->file);
                    return -1;
                }

                DEBG2("--> read %i bytes ", read_chunk);
                DEBG2("=====> read %s ", (char*)(*out_data));
            }

            fclose(file_d);

            DEBG2("file %s closed.", task_data->file);
        }

    }

    memset(*out_data+(task_data->n_reads*read_chunk),"\0",1);
    DEBG2("output: %s", (char*)*out_data);

    DEBG1("..reading done!");

    return 0;

}


/* free a iotask_read */
static int free_data_iotask_read(void* data){

    IOTaskReadInputData* task_data = data;

    free(task_data->name);
    free(task_data->file);

    /* finally free everything */
    free(task_data);
}


/* init functor */
IOTaskFunctor* init_iotask_read(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskReadInputData* data;

    DEBG1("initialising iotask read..");

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskReadInputData*)malloc(sizeof(IOTaskReadInputData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_reads", &(data->n_reads));

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_read_file;
    functor->free_data = free_data_iotask_read;
    functor->get_name = get_name_taskr;

    DEBG1("initialisation done!");

    return functor;

}
