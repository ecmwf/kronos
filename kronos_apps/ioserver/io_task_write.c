
#include "ioserver/io_task_write.h"
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
 * get task name
 */
static const char* get_name_taskw(const void* data){
    const IOTaskWriteInputData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}



/*
 * do write on file
 */
static int do_write_to_file(int fd,
                            const long int* size,
                            long int* actual_number_writes,
                            void* raw_data) {

    long remaining;
    long chunk_size;
    int result;
    long int bytes_to_write;

    /* Loop over chunks of the maximum chunk size, until all written */
    remaining = *size;
    while (remaining > 0) {

        chunk_size = remaining < MAX_WRITE_SIZE ? remaining : MAX_WRITE_SIZE;
        assert(chunk_size > 0);
        DEBG2("Writing chunk of: %li bytes", chunk_size);

        bytes_to_write = chunk_size;
        while (bytes_to_write > 0) {

            DEBG2("----> writing: %li bytes", bytes_to_write);
            result = write(fd, (char*)raw_data, bytes_to_write);
            DEBG2("----> result: %i", result);
            (*actual_number_writes)++;

            if (result == -1) {
                ERRO3("A write error occurred: %d (%s)", errno, strerror(errno));
                return -1;
                break;
            }

            bytes_to_write -= result;
        }

        remaining -= chunk_size;
    }

    return 0;
}


/*
 * execute a write
 * NB: caller has ownership of "out_data"
 */
static int execute_iotask_write(void* data, IOData** out_data){

    IOTaskWriteInputData* task_data = data;

    int fd;
    int open_flags;
    long int actual_writes = 0;
    long int per_write_size;
    long int remaining_data;
    long int offset;

    char* raw_content=(char*)(task_data->data_to_write->content);        

    DEBG1("--> executing writing.. ");

    /* no output data, it's a writing task */
    *out_data = NULL;

    /* write op count is such that last write might be smaller */
    if (task_data->n_bytes%task_data->n_writes){
        per_write_size = task_data->n_bytes/(task_data->n_writes-1);
    } else {
        per_write_size = task_data->n_bytes/task_data->n_writes;
    }
    remaining_data = task_data->n_bytes;

    open_flags = O_WRONLY | O_CREAT;
    if (!strcmp(task_data->mode, "append")){
        open_flags = O_WRONLY | O_CREAT | O_APPEND;
    }

    DEBG2("writing a total of %li bytes", task_data->n_bytes);

    /* do the writing quite aggressively open-seek-write-close */
    offset = 0;
    while(remaining_data>0){

        DEBG2("opening file %s", task_data->file);
        fd = open(task_data->file, open_flags, S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd == -1) {
            ERRO2("An error occurred opening the file %s for write: %d (%s)", task_data->file);
            return -1;
        }

        DEBG2("seeking file offset %i", offset);
        lseek(fd, offset, SEEK_SET);

        DEBG3("writing chunk of %li bytes, offset=%i", per_write_size, offset);
        DEBG2("remaining_data %li", remaining_data);

        per_write_size = (remaining_data/per_write_size != 0)? per_write_size : remaining_data % per_write_size;
        do_write_to_file(fd, &per_write_size, &actual_writes, raw_content+offset);

        remaining_data -= per_write_size;
        offset += per_write_size;

        /* close the file */
        if (close(fd) == -1){
            ERRO2("An error occurred closing the file %s: %d (%s)", task_data->file);
            return -1;
        } else {
            DEBG2("file %s closed.", task_data->file);
        }
    }

    return 0;

}



/*
 * free an IOtask_write
 */
static int free_data_iotask_write(void* data){

    IOTaskWriteInputData* task_data = data;

    free(task_data->name);
    free(task_data->file);
    free(task_data->mode);

    free_iodata(task_data->data_to_write);

    /* finally free everything */
    free(task_data);
}



/*
 * init functor
 */
IOTaskFunctor* init_iotask_write(const JSON* config, void* msg_data){

    IOTaskFunctor* functor;
    IOTaskWriteInputData* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskWriteInputData*)malloc(sizeof(IOTaskWriteInputData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );
    json_as_string_ptr(json_object_get(config, "mode"), (const char**)(&(data->mode)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_writes", &(data->n_writes));

    /* add the actual data to write into the task input */
    DEBG1("bytes to create");
    DEBG2("bytes to create %i", data->n_bytes);
    if ((data->data_to_write = fill_iodata(data->n_bytes, msg_data))==NULL){
        ERRO1("data filling up failed!");
    }

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_iotask_write;
    functor->free_data = free_data_iotask_write;
    functor->get_name = get_name_taskw;

    return functor;

}

