
#include "io_executor.h"

#include "io_task.h"
#include "common/logger.h"
#include "common/json.h"

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>


/* do write on file */
int _do_write_to_file(int fd,
                     const long int* size,
                     long int* actual_number_writes) {

    long remaining;
    long chunk_size;
    int result;
    char* buffer = 0;
    long int bytes_to_write;

    /* Loop over chunks of the maximum chunk size, until all written */
    remaining = *size;
    while (remaining > 0) {

        chunk_size = remaining < MAX_WRITE_SIZE ? remaining : MAX_WRITE_SIZE;
        assert(chunk_size > 0);
        DEBG2("Writing chunk of: %li bytes", chunk_size);

        buffer = malloc(chunk_size);

        bytes_to_write = chunk_size;
        while (bytes_to_write > 0) {

            DEBG2("----> writing: %li bytes", bytes_to_write);
            result = write(fd, buffer, bytes_to_write);
            DEBG2("----> result: %i", result);
            (*actual_number_writes)++;


            if (result == -1) {
                ERRO3("A write error occurred: %d (%s)", errno, strerror(errno));
                return -1;
                break;
            }

            bytes_to_write -= result;
        }

        free(buffer);
        buffer = 0;
        remaining -= chunk_size;
    }

    return 0;
}


/* open-write-close file */
int _write_file(const char* file_name,
               const long int* n_bytes,
               const long int* offset,
               const long int* n_writes,
               const char* write_mode){

    int fd;
    int open_flags;
    int iwrite;
    long int actual_writes = 0;
    long int per_write_size = *n_bytes / *n_writes;

    DEBG1("--> executing writing.. ");

    open_flags = O_WRONLY | O_CREAT;
    if (!strcmp(write_mode, "append")){
        open_flags = O_WRONLY | O_CREAT | O_APPEND;
    }

    DEBG2("opening file %s", file_name);
    fd = open(file_name, open_flags, S_IRUSR | S_IWUSR | S_IRGRP);

    if (fd == -1) {
        ERRO2("An error occurred opening the file %s for write: %d (%s)", file_name);
        return -1;
    }

    /* do the writing (NOTE: offset=0 at the moment).. */
    DEBG2("writing a total of %li bytes", *n_bytes);
    for (iwrite=0; iwrite<*n_writes; iwrite++){

        DEBG2("writing chunk of %li bytes", per_write_size);
        _do_write_to_file(fd, &per_write_size, &actual_writes);
    }

    /* close the file */
    if (close(fd) == -1){
        ERRO2("An error occurred closing the file %s: %d (%s)", file_name);
        return -1;
    } else {
        DEBG2("file %s closed.", file_name);
    }

    return 0;

}


/* open-read-close */
int _read_file(const char* file_name,
              const long int* n_bytes,
              const long int* n_reads,
              const long int* offset){

    FILE* file;
    long result;
    int ret;
    long int iread;
    long int read_chunk = *n_bytes / *n_reads;
    char* read_buffer;

    DEBG1("--> executing reading.. ");

    read_buffer = malloc(*n_reads * read_chunk);

    /* open-read-close */
    for (iread=0; iread<*n_reads; iread++)
    {
        file = fopen(file_name, "rb");
        if (file != NULL) {

            ret = fseek(file, *offset, SEEK_SET);

            if (ret == -1) {
                ERRO2("A error occurred seeking in file: %s (%d) %s", file_name);
            } else {

                result = fread(read_buffer, 1, read_chunk, file);

                if (result != read_chunk) {
                    ERRO2("A read error occurred reading file: %s (%d) %s", file_name);
                }
            }

            fclose(file);
        }

    }

    free(read_buffer);

    return 0;

}


/* Execute an I/O task */
int execute_io_task(const char* io_json_str, const char* io_data){

    IOTask* iotask;

    const char* _io_task_type;
    const char* _io_task_file;
    const char* _io_task_mode;

    long int _io_task_bytes;
    long int _io_task_nwrites;
    long int _io_task_nreads;
    long int _io_task_offset;

    DEBG1("Executing IO task");

    iotask = iotask_from_json_string(io_json_str);

    _io_task_type = iotask->type;
    _io_task_file = iotask->file;
    _io_task_mode = iotask->write_mode;

    _io_task_bytes = iotask->n_bytes;
    _io_task_nwrites = iotask->n_writes;
    _io_task_nreads = iotask->n_reads;
    _io_task_offset = iotask->offset;

    if (!strcmp(_io_task_type, "writer")){

        _write_file(_io_task_file,
                    &_io_task_bytes,
                    &_io_task_offset,
                    &_io_task_nwrites,
                    _io_task_mode);

    } else if (!strcmp(_io_task_type, "reader")) {

        _read_file(_io_task_file,
                   &_io_task_bytes,
                   &_io_task_nreads,
                   &_io_task_offset);
    }

    free(iotask);

    return 0;
}