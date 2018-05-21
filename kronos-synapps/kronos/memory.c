/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <stdlib.h>
#include "kronos/memory.h"
#include "kronos/global_config.h"
#include "kronos/stats.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_logger("memory");

    return logger;
}

static TimeSeriesLogger* flops_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("alloc_mem_kb");

    return logger;
}

MEMParamsInternal get_mem_params(const MEMConfig* config) {

    const GlobalConfig* global_conf = global_config_instance();

    MEMParamsInternal params;

    /* the memory kernel already defines memory allocated per process
    so, no need for dividing by the number of processes..*/
    /*params.node_mem = config->mem_kb*1024./ global_conf->nprocs;*/
    params.node_mem = config->mem_kb*1024.;

    return params;
}


static int execute_mem(const void* data) {

    const MEMConfig* config = data;
    MEMParamsInternal params;
    int error;

    size_t mem_length;
    int page_size = sysconf(_SC_PAGESIZE);
    long n_bytes;
    char *alloc_mem_b;

    error = 0;

    params = get_mem_params(config);

    TRACE3("Executing MEMs: %li, per MPI process: %li", config->mem_kb, params.node_mem);

    stats_start(stats_instance());

    n_bytes = params.node_mem/page_size > 0? (params.node_mem/page_size+1) * page_size : page_size;

    /* allocate the required memory */
    alloc_mem_b = (char *) malloc(n_bytes);
    if (!alloc_mem_b){
        fprintf(stderr, "An error occurred during memory allocation\n");
        error = -1;
        return error;
    }

    /* set mem to an arbitrary value */
    memset(alloc_mem_b, 'b', n_bytes);

    TRACE2("----> sysconf(_SC_PAGESIZE): %li", sysconf(_SC_PAGESIZE));
    TRACE2("----> n_bytes: %li", n_bytes);
    /* TRACE2("----> alloc_mem_b: %c", alloc_mem_b[0]); */

    /* Log time series data */
    stats_stop_log(stats_instance(), params.node_mem);
    log_time_series_add_chunk_data(flops_time_series(), params.node_mem);
    log_time_series_chunk();

    /* free the allocated memory */
    free(alloc_mem_b);

    return error;
}


KernelFunctor* init_mem(const JSON* config_json) {

    MEMConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(MEMConfig));

    if (json_object_get_integer(config_json, "mem_proc_kb", &config->mem_kb) != 0 ||
        config->mem_kb < 0) {

        fprintf(stderr, "Invalid parameters specified in mem config\n");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_mem;
    functor->data = config;

    return functor;
}
