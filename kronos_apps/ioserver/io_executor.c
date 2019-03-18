
#include "io_executor.h"

#include "io_task.h"
#include "common/logger.h"
#include "common/json.h"

#include <stdio.h>
#include <string.h>

#ifdef HAVE_PMEMIO
  #include "libpmem.h"
  #include "libpmemobj.h"
  #include "nvram_layout.h"
#endif

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#include "limits.h"


#define BUF_LEN 1048576


/* do write on file */
static int do_write_to_file(int fd,
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
static int write_file(const char* file_name,
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
        do_write_to_file(fd, &per_write_size, &actual_writes);
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


/* write this file to nvram (through memory map) */
static int write_file_to_nvram(const char* file_name,
                               const long int* n_bytes,
                               const long int* pool_size){

#ifdef HAVE_PMEMIO

    char* file_buffer;
    char mapped_file_name[PATH_MAX];

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    size_t poolsize_actual = (size_t)(*pool_size * PMEMOBJ_MIN_POOL);

    DEBG2( "NVRAM poolsize requested: %i\n", poolsize_actual);
    if ((poolsize_actual > PMEMOBJ_MIN_POOL*100) || (poolsize_actual < 0)){
        poolsize_actual = PMEMOBJ_MIN_POOL*100;
    }
    DEBG2( "NVRAM actual poolsize: %i\n", poolsize_actual);


    /* get file name */
    strncpy(mapped_file_name, file_name, strlen(file_name));
    mapped_file_name[strlen(file_name)] = '\0';
    DEBG2( "NVRAM mapped_file_name: %s\n", mapped_file_name);

    /* get file name fill up the buffer (TODO: just dummy content at the moment..) */
    DEBG2("NVRAM btes to write: %i", *n_bytes);
    file_buffer = (char*)malloc(*n_bytes+1);
    memset(file_buffer, 'v', *n_bytes);
    file_buffer[*n_bytes] = '\0';
    DEBG2("NVRAM file buffer: %s", file_buffer);

    DEBG2( "NVRAM mapped_file_name: %s\n", mapped_file_name);

    /* create the pool with the proper name/permissions */
    pop = pmemobj_create(mapped_file_name,
                         LAYOUT_NAME,
                         poolsize_actual,
                         0666);

    if (pop == NULL) {
        ERRO1( "NVRAM: error generating pool!");
        perror("pmemobj_create");
        exit(1);
    }

    DEBG1( "NVRAM: pool created");

    /* get the root pointer and cast it as appropriate */
    root = pmemobj_root(pop, sizeof(struct kronos_pobj_root));
    rootp = pmemobj_direct(root);

    DEBG1( "NVRAM: root pointer acquired");

    /* transactional write to range */
    DEBG2( "NVRAM: starting transaction: writing (%i) bytes..", strlen(file_buffer));
    TX_BEGIN(pop) {
        pmemobj_tx_add_range(root, 0, sizeof(struct kronos_pobj_root));
        memcpy(rootp->buf, file_buffer, strlen(file_buffer));
    } TX_END
    DEBG1( "NVRAM: transaction completed");

    /* finally close the pool */
    pmemobj_close(pop);

    DEBG1( "NVRAM: persistent object pool closed.");


#endif

    return 0;

}


/* open-read-close */
static int read_file(const char* file_name,
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

                DEBG2("--> read %i bytes ", read_chunk);
            }

            fclose(file);

            DEBG2("file %s closed.", file_name);
        }

    }

    DEBG1("freeing reading buffer..");
    free(read_buffer);

    DEBG1("..reading done!");

    return 0;

}


/* open-read-close */
static int read_file_from_nvram(const char* file_name,
                                const long int* n_bytes,
                                const long int* n_reads,
                                const long int* offset){

#ifdef HAVE_PMEMIO

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    int i;

    DEBG2("NVRAM: opening pool %s", file_name);
    pop = pmemobj_open(file_name, LAYOUT_NAME);
    if (pop == NULL) {
        perror("pmemobj_open");
        ERRO1( "NVRAM: error opening pool!");
        return 1;
    }

    root = pmemobj_root(pop, sizeof(struct kronos_pobj_root));
    rootp = pmemobj_direct(root);
    DEBG1( "NVRAM: root pointer acquired");

    DEBG2("NVRAM buffer read: %i", sizeof(rootp->buf));
    for (i=0; i<sizeof(rootp->buf); i++){
        DEBG2("NVRAM buffer read: %c", rootp->buf[i]);
    }

    DEBG1( "NVRAM: closing pool..");
    pmemobj_close(pop);
    DEBG1( "NVRAM: pool closed!");

#endif

    return 0;

}


/* Execute an I/O task */
int execute_io_task(IOTask* iotask){


    const char* io_task_type;
    const char* io_task_file;
    const char* io_task_mode;

    long int io_task_bytes;
    long int io_task_nwrites;
    long int io_task_poolsize;
    long int io_task_nreads;
    long int io_task_offset;

    DEBG1("Executing IO task");

    io_task_type = iotask->type;
    io_task_file = iotask->file;
    io_task_mode = iotask->write_mode;

    io_task_bytes = iotask->n_bytes;
    io_task_nwrites = iotask->n_writes;
    io_task_poolsize = iotask->pool_size;
    io_task_nreads = iotask->n_reads;
    io_task_offset = iotask->offset;

    if (!strcmp(io_task_type, "writer")){

        write_file(io_task_file,
                    &io_task_bytes,
                    &io_task_offset,
                    &io_task_nwrites,
                    io_task_mode);

    } else if (!strcmp(io_task_type, "nvram_writer")) {

        write_file_to_nvram(io_task_file,
                            &io_task_bytes,
                            &io_task_poolsize);

    } else if (!strcmp(io_task_type, "reader")) {

        read_file(io_task_file,
                   &io_task_bytes,
                   &io_task_nreads,
                   &io_task_offset);

    } else if (!strcmp(io_task_type, "nvram_reader")) {

        read_file_from_nvram(io_task_file,
                             &io_task_bytes,
                             &io_task_nreads,
                             &io_task_offset);
    } else {
        INFO2("io task type %s not recognised [reader|writer|nvram_writer]", io_task_type);
        return 1;
    }

    return 0;
}



/* Execute an I/O task */
int execute_io_task_from_string(const char* io_json_str){

    IOTask* iotask;
    int iotask_err;

    DEBG1("Executing IO task from string..");

    iotask = iotask_from_json_string(io_json_str);

    /* do execute the task */
    iotask_err = execute_io_task(iotask);
    if (iotask_err){
        DEBG1("error in executing IO task!");
        free(iotask);
        iotask = NULL;
        return 1;
    }

    free(iotask);
    iotask = NULL;
    return 0;
}
