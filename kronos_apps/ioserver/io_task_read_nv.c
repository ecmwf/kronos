
#include "ioserver/io_task_read_nv.h"
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


/*
 * get task name
 */
static const char* get_name_taskrnv(const void* data){
    const IOTaskReadNVRAMInputData* task_data = data;
    assert(task_data != NULL);
    return task_data->name;
}


/*
 * open-read-close
 */
static int execute_read_file_nvram(void* data, IOData** out_data){

#ifdef HAVE_PMEMIO

    PMEMobjpool *pop;
    PMEMoid root;
    struct kronos_pobj_root *rootp;
    int i;

    IOTaskReadNVRAMInputData* task_data = data;

    /*prepare to accept output data */
    *out_data = malloc(sizeof(IOData));

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

    /* fill up the output data read to be returned */
    (*out_data)->size = 999; /*TODO fix this!! */
    (*out_data)->content = (void*)rootp->buf;

    DEBG1( "NVRAM: closing pool..");
    pmemobj_close(pop);
    DEBG1( "NVRAM: pool closed!");

#endif

    return 0;

}


/*
 * execute a write
 */
static int free_data_iotask_read_nv(void* data){

    IOTaskReadNVRAMInputData* task_data = data;

    free(task_data->name);
    free(task_data->file);

    /* finally free everything */
    free(task_data);
}


/*
 * init functor
 */
IOTaskFunctor* init_iotask_read_nvram(const JSON* config){

    IOTaskFunctor* functor;
    IOTaskReadNVRAMInputData* data;

    functor = malloc(sizeof(IOTaskFunctor));
    data = (IOTaskReadNVRAMInputData*)malloc(sizeof(IOTaskReadNVRAMInputData));

    /* fill up the write functor as appropriate */
    json_as_string_ptr(json_object_get(config, "name"), (const char**)(&(data->name)) );
    json_as_string_ptr(json_object_get(config, "file"), (const char**)(&(data->file)) );

    json_object_get_integer(config, "n_bytes", &(data->n_bytes));
    json_object_get_integer(config, "offset", &(data->offset));
    json_object_get_integer(config, "n_reads", &(data->n_reads));

    /* prepare the functor */
    functor->data = data;
    functor->execute = execute_read_file_nvram;
    functor->free_data = free_data_iotask_read_nv;
    functor->get_name = get_name_taskrnv;

    return functor;

}
