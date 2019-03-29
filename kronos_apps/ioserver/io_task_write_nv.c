
#include "ioserver/io_task_write_nv.h"
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


/*
 * get task name
 */
static const char* get_name_taskwnv(const void* data){
    const IOTaskWriteNVRAMInputData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}


/*
 * write this file to nvram (through memory map)
 */
static int execute_iotask_write_nvram(void* data, IOData** out_data){

#ifdef HAVE_PMEMIO

    char* file_buffer;
    char mapped_file_name[PATH_MAX];
    IOTaskWriteNVRAMInputData* task_data = data;

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    size_t poolsize_actual = (size_t)(task_data->pool_size * PMEMOBJ_MIN_POOL);

    /* no output data, it's a writing task */
    *out_data = NULL;

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



/*
 * free an IOtask_write_nv
 */
static int free_data_iotask_write_nv(void* data){

    IOTaskWriteNVRAMInputData* task_data = data;

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
IOTaskFunctor* init_iotask_write_nvram(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskWriteNVRAMInputData* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskWriteNVRAMInputData*)malloc(sizeof(IOTaskWriteNVRAMInputData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );
    json_as_string_ptr(json_object_get(config, "mode"), (const char**)(&(data->mode)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_writes", &(data->n_writes));
    json_object_get_integer(config, "pool_size", &(data->n_writes));

    /* add the actual raw data to write */
    data->data_to_write = (IOData*)malloc(sizeof(IOData));
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
