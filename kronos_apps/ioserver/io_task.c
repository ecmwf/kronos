
#include "io_task.h"
#include "common/json.h"
#include "common/logger.h"

#include <stdio.h>
#include <string.h>

#ifdef HAVE_PMEMIO
  #include "libpmem.h"
  #include "libpmemobj.h"
  #include "nvram_layout.h"
#endif

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <assert.h>
#include "limits.h"




/* ==================== IO_TASK_WRITE ==================== */

static const char* get_name_taskw(const void* data){
    const IOTaskWriteData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}

/* do write on file */
static int do_write_to_file(int fd,
                            const long int* size,
                            long int* actual_number_writes,
                            void* raw_data) {

    long remaining;
    long chunk_size;
    int result;
    long int bytes_to_write;
    char* raw_data_char = (char*)raw_data;

    /* Loop over chunks of the maximum chunk size, until all written */
    remaining = *size;
    while (remaining > 0) {

        chunk_size = remaining < MAX_WRITE_SIZE ? remaining : MAX_WRITE_SIZE;
        assert(chunk_size > 0);
        DEBG2("Writing chunk of: %li bytes", chunk_size);

        bytes_to_write = chunk_size;
        while (bytes_to_write > 0) {

            DEBG2("----> writing: %li bytes", bytes_to_write);
            result = write(fd, raw_data_char, bytes_to_write);
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


/* execute a write */
static int execute_iotask_write(void* data){

    IOTaskWriteData* task_data = data;

    int fd;
    int open_flags;
    long int actual_writes = 0;
    long int per_write_size;
    long int remaining_data;
    long int offset;

    char* raw_content=(char*)(task_data->data_to_write->content);

    DEBG1("--> executing writing.. ");

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

    DEBG2("opening file %s", task_data->file);
    fd = open(task_data->file, open_flags, S_IRUSR | S_IWUSR | S_IRGRP);

    if (fd == -1) {
        ERRO2("An error occurred opening the file %s for write: %d (%s)", task_data->file);
        return -1;
    }

    DEBG2("writing a total of %li bytes", task_data->n_bytes);
    offset = 0;
    while(remaining_data>0){

        DEBG3("writing chunk of %li bytes, offset=%i", per_write_size, offset);
        DEBG2("remaining_data %li", remaining_data);

        per_write_size = (remaining_data/per_write_size != 0)? per_write_size : remaining_data % per_write_size;
        do_write_to_file(fd, &per_write_size, &actual_writes, raw_content+offset);

        remaining_data -= per_write_size;
        offset += per_write_size;
    }

    /* close the file */
    if (close(fd) == -1){
        ERRO2("An error occurred closing the file %s: %d (%s)", task_data->file);
        return -1;
    } else {
        DEBG2("file %s closed.", task_data->file);
    }

    return 0;

}


/* execute a write */
static int free_data_iotask_write(void* data){

    IOTaskWriteData* task_data = data;

    free(task_data->name);
    free(task_data->file);
    free(task_data->mode);

    free_sized_data(task_data->data_to_write);

    /* finally free everything */
    free(task_data);
}



/* init functor */
static IOTaskFunctor* init_iotask_write(const JSON* config, void* msg_data){

    IOTaskFunctor* functor;
    IOTaskWriteData* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskWriteData*)malloc(sizeof(IOTaskWriteData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );
    json_as_string_ptr(json_object_get(config, "mode"), (const char**)(&(data->mode)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_writes", &(data->n_writes));

    /* add the actual raw data to write */
    data->data_to_write = (SizedData*)malloc(sizeof(SizedData));
    if (data->data_to_write == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_to_write->content = (void*)malloc( data->n_bytes * sizeof(char));
    if (data->data_to_write->content == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_to_write->size = data->n_bytes;
    data->data_to_write->content = msg_data;

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_iotask_write;
    functor->free_data = free_data_iotask_write;
    functor->get_name = get_name_taskw;

    return functor;

}
/* ================== IO_TASK_WRITE (END) ================== */




/* ==================== IO_TASK_WRITE_NVRAM ==================== */

static const char* get_name_taskwnv(const void* data){
    const IOTaskWriteDataNVRAM* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}

/* write this file to nvram (through memory map) */
static int execute_iotask_write_nvram(void* data){

#ifdef HAVE_PMEMIO

    char* file_buffer;
    char mapped_file_name[PATH_MAX];
    IOTaskWriteDataNVRAM* task_data = data;

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    size_t poolsize_actual = (size_t)(task_data->pool_size * PMEMOBJ_MIN_POOL);

    DEBG2( "NVRAM poolsize requested: %i\n", poolsize_actual);
    if ((poolsize_actual > PMEMOBJ_MIN_POOL*100) || (poolsize_actual < 0)){
        poolsize_actual = PMEMOBJ_MIN_POOL*100;
    }
    DEBG2( "NVRAM actual poolsize: %i\n", poolsize_actual);

    /* get file name */
    strncpy(mapped_file_name, task_data->file, strlen(task_data->file));
    mapped_file_name[strlen(task_data->file)] = '\0';
    DEBG2( "NVRAM mapped_file_name: %s\n", mapped_file_name);

    /* get file name fill up the buffer (TODO: just dummy content at the moment..) */
    DEBG2("NVRAM btes to write: %i", task_data->n_bytes);
    file_buffer = (char*)malloc(task_data->n_bytes+1);
    memset(file_buffer, 'v', task_data->n_bytes);
    file_buffer[task_data->n_bytes] = '\0';
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


/* execute a write */
static int free_data_iotask_write_nv(void* data){

    IOTaskWriteData* task_data = data;

    free(task_data->name);
    free(task_data->file);
    free(task_data->mode);

    free_sized_data(task_data->data_to_write);

    /* finally free everything */
    free(task_data);
}


/* init functor */
static IOTaskFunctor* init_iotask_write_nvram(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskWriteDataNVRAM* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskWriteDataNVRAM*)malloc(sizeof(IOTaskWriteDataNVRAM));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );
    json_as_string_ptr(json_object_get(config, "mode"), (const char**)(&(data->mode)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_writes", &(data->n_writes));
    json_object_get_integer(config, "pool_size", &(data->n_writes));

    /* add the actual raw data to write */
    data->data_to_write = (SizedData*)malloc(sizeof(SizedData));
    if (data->data_to_write == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_to_write->content = malloc( data->n_bytes * sizeof(char));
    if (data->data_to_write->content == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_to_write->size = data->n_bytes;
    /* data->data_to_write->content = msg_data; TODO: fix this..*/

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_iotask_write_nvram;
    functor->free_data = free_data_iotask_write_nv;
    functor->get_name = get_name_taskwnv;

    return functor;

}
/* ================== IO_TASK_WRITE_NVRAM (END) ================== */


/* ======================== IO_TASK_READ ========================= */

static const char* get_name_taskr(const void* data){
    const IOTaskReadData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}

/* open-read-close */
static int execute_read_file(void* data){

    IOTaskReadData* task_data = data;

    FILE* file_d;
    long result;
    int ret;
    long int iread;
    long int read_chunk = task_data->n_bytes / task_data->n_reads;

    DEBG1("--> executing reading.. ");

    /* allocate space for the output */
    DEBG2("allocating %i bytes", task_data->n_reads * read_chunk);
    task_data->data_read = malloc(task_data->n_reads * read_chunk);
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
                result = fread( (char*)(task_data->data_read) + task_data->offset,
                                1,
                                read_chunk,
                                file_d);

                if (result != read_chunk) {
                    ERRO2("A read error occurred reading file: %s (%d) %s", task_data->file);
                    return -1;
                }

                DEBG2("--> read %i bytes ", read_chunk);
                DEBG2("=====> read %s ", (char*)(task_data->data_read));
            }

            fclose(file_d);

            DEBG2("file %s closed.", task_data->file);
        }

    }

    DEBG1("..reading done!");

    return 0;

}


/* execute a write */
static int free_data_iotask_read(void* data){

    IOTaskReadData* task_data = data;

    free(task_data->name);
    free(task_data->file);

    free_sized_data(task_data->data_read);

    /* finally free everything */
    free(task_data);
}



/* init functor */
static IOTaskFunctor* init_iotask_read(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskReadData* data;

    DEBG1("initialising iotask read..");

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskReadData*)malloc(sizeof(IOTaskReadData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_reads", &(data->n_reads));

    /* add the actual raw data to be read */
    data->data_read = (SizedData*)malloc(sizeof(SizedData));
    if (data->data_read == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_read->size = data->n_bytes;
    data->data_read->content = NULL;

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_read_file;
    functor->free_data = free_data_iotask_read;
    functor->get_name = get_name_taskr;

    DEBG1("initialisation done!");

    return functor;

}
/* ================== IO_TASK_READ (END) ================== */


/* ==================== IO_TASK_READ_NVRAM ==================== */

static const char* get_name_taskrnv(const void* data){
    const IOTaskReadDataNVRAM* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}

/* open-read-close */
static int execute_read_file_nvram(void* data){

#ifdef HAVE_PMEMIO

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    int i;

    IOTaskReadDataNVRAM* task_data = data;

    DEBG2("NVRAM: opening pool %s", task_data->file);
    pop = pmemobj_open(task_data->file, LAYOUT_NAME);
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


/* execute a write */
static int free_data_iotask_read_nv(void* data){

    IOTaskReadDataNVRAM* task_data = data;

    free(task_data->name);
    free(task_data->file);
    free_sized_data(task_data->data_read);

    /* finally free everything */
    free(task_data);
}


/* init functor */
static IOTaskFunctor* init_iotask_read_nvram(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskReadDataNVRAM* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskReadDataNVRAM*)malloc(sizeof(IOTaskReadDataNVRAM));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_reads", &(data->n_reads));

    /* add the actual raw data to be read */
    data->data_read = (SizedData*)malloc(sizeof(SizedData));
    if (data->data_read == NULL) {
        DEBG1("memory allocation error");
    }

    data->data_read->size = data->n_bytes;
    data->data_read->content = NULL;

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_read_file_nvram;
    functor->free_data = free_data_iotask_read_nv;
    functor->get_name = get_name_taskrnv;

    return functor;

}
/* ================== IO_TASK_READ_NVRAM (END) ================== */








/* ===================================================== */
/* ================== IO_TASK FACTORY ================== */
/* ===================================================== */

/* IO task factory from json */
IOTaskFunctor* iotask_factory_from_json(const JSON* config, void* msg_data){

    const char* iotask_name;
    json_as_string_ptr(json_object_get(config, "name"), &iotask_name);

    DEBG2("got iotask of name: %s", iotask_name);

    if (!strcmp(iotask_name, "writer")){
        return init_iotask_write(config, msg_data);

    } else if (!strcmp(iotask_name, "nvram_writer")){
        return init_iotask_write_nvram(config);

    } else if (!strcmp(iotask_name, "reader")){
        return init_iotask_read(config);

    } else if (!strcmp(iotask_name, "nvram_reader")){
        return init_iotask_read_nvram(config);
    }

    ERRO2("IO task %s not recognised!", iotask_name);

    return NULL;
}

/* IO task factory from string */
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


/* free sized data */
void free_sized_data(SizedData* data){
    free(data->content);
    free(data);
}


