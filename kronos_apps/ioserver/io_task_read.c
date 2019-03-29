
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
    long int iread_total;
    long int read_chunk = task_data->n_bytes / task_data->n_reads;
    char* read_tmp;
    char* reading_ptr;
    long int offset_total;

    DEBG1("--> executing reading.. ");

    /* allocate space for the output */
    DEBG2("allocating %i bytes", task_data->n_reads * read_chunk + 1);
    (*out_data)->size = task_data->n_reads * read_chunk;
    (*out_data)->size_as_string = task_data->n_reads * read_chunk + 1;
    (*out_data)->content = malloc(task_data->n_reads * read_chunk + 1);
    DEBG2("%i bytes allocated", task_data->n_reads * read_chunk + 1);

    /* open-seek-read-close */
    iread_total = 0;
    for (iread=0; iread<task_data->n_reads; iread++)
    {

        file_d = fopen(task_data->file, "rb");
        DEBG2("file %s open", task_data->file);

        if (file_d != NULL) {

            /* at each read move to the correct offset */
            offset_total = task_data->offset+iread*read_chunk;
            ret = fseek(file_d, offset_total, SEEK_SET);

            DEBG2("--> trying to read %i bytes ", read_chunk);
            if (ret == -1) {
                ERRO2("A error occurred seeking in file: %s (%d) %s", task_data->file);
                return -1;
            } else {

                /* read from file from the proper offset location */
                reading_ptr = (char*)((*out_data)->content) + offset_total;
                result = fread( reading_ptr, 1, read_chunk, file_d);

                if (result != read_chunk) {
                    ERRO2("A read error occurred reading file: %s (%d) %s", task_data->file);
                    return -1;
                }

                DEBG2("--> read %i bytes ", result);

                /* try to display what has been read */
                read_tmp = malloc(result+1);
                strncpy(read_tmp, reading_ptr, result);
                strncpy(read_tmp+result, "\0", 1);
                DEBG2("=====> read %s ", read_tmp);
                free(read_tmp);
                read_tmp = NULL;
                /* --------------------------------- */

                iread_total += result;
            }

            fclose(file_d);
            DEBG2("file %s closed.", task_data->file);
        }

    }

    DEBG2("check on total bytes read: %i.", iread_total);

    memset((char*)((*out_data)->content)+iread_total,'\0',1);
    DEBG2("output: %s", (char*)((*out_data)->content));

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
