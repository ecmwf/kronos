/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifdef HAVE_PMEMIO

#include <stdlib.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include "libpmem.h"
#include "libpmemobj.h"

#include "kronos/global_config.h"
#include "kronos/memory_persist.h"
#include "kronos/stats.h"
#include "kronos/trace.h"
#include "common/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_logger("memory_persist");

    return logger;
}

static TimeSeriesLogger* flops_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("alloc_mem_kb_persist");

    return logger;
}

MEMPersistParamsInternal get_MEMPersist_params(const MEMPersistConfig* config) {

    MEMPersistParamsInternal params;
    long max_alloc;
    const char* env;

    /*params.node_mem = config->mem_kb*1024./ global_conf->nprocs;*/
    params.node_mem = config->mem_kb*1024.;

    if ((env = getenv("KRONOS_PROC_MAX_MALLOC")) != 0) {

        max_alloc = strtol(env, NULL, 10);
        if (params.node_mem > max_alloc) {
            fprintf(stderr, "*** WARNING: malloc size constrained by environment variable KRONOS_PROC_MAX_MALLOC ****");
            fprintf(stderr, "malloc requested: %li bytes, will allocate: %li bytes", params.node_mem, max_alloc);
            params.node_mem = max_alloc;
        }
    }

    return params;
}

#define BUF_LEN 4194304

static int execute_mem_persist(const void* data) {

    const MEMPersistConfig* config = data;
    const GlobalConfig* global_config = global_config_instance();
    MEMPersistParamsInternal params;

    int page_size = sysconf(_SC_PAGESIZE);
    long n_bytes;
    char *pmemaddr;

    char file_buffer[BUF_LEN];
    int cc;
    int is_pmem;

    size_t mapped_len;
    char mapped_file_name[80];

    params = get_MEMPersist_params(config);

    stats_start(stats_instance());

    /* #bytes that this process allocates */
    n_bytes = params.node_mem/page_size > 0? (params.node_mem/page_size+1) * page_size : page_size;

    /* copying some bytes on this buffer */
    memset(file_buffer, 'v', n_bytes);

    TRACE3("Executing MEM PERSISTs: %li, per MPI process: %li", config->mem_kb, params.node_mem);
    TRACE2( "PATH_MAX: %i\n", PATH_MAX);

    /* ========= memory mapped file =========== */
    strcpy(mapped_file_name, global_config->nvdimm_root_path);
    strcat(mapped_file_name, "/");
    strcat(mapped_file_name, "mapped_file_test");
    TRACE2( "NVRAM mapped_file_name: %s\n", mapped_file_name);

    /* create a pmem file and memory map it */
    if ((pmemaddr = pmem_map_file(mapped_file_name,
                                  n_bytes,
                                  PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                  0666,
                                  &mapped_len,
                                  &is_pmem)) == NULL)
    {
        perror("pmem_map_file");
        exit(1);
    }

    TRACE2( "asked: n_bytes: %i\n", n_bytes);
    TRACE2( "mapped: mapped_len: %i\n", mapped_len);

    /* write it to the pmem */
    if (is_pmem) {
        TRACE1( "Real PMEM found, persisting..");
        pmem_memcpy_persist(pmemaddr, file_buffer, cc);
    } else {
        TRACE1( "No real PMEM found, persisting..");
        memcpy(pmemaddr, file_buffer, cc);
        pmem_msync(pmemaddr, cc);
    }

    pmem_unmap(pmemaddr, mapped_len);

    /* Log time series data */
    stats_stop_log(stats_instance(), params.node_mem);
    log_time_series_add_chunk_data(flops_time_series(), params.node_mem);
    log_time_series_chunk();

    return 0;
}


KernelFunctor* init_mem_persist_kernel(const JSON* config_json) {

    MEMPersistConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(MEMPersistConfig));

    if (json_object_get_integer(config_json, "kb_mem", &config->mem_kb) != 0 ||
        config->mem_kb < 0) {

        fprintf(stderr, "Invalid parameters specified in mem config\n");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_mem_persist;
    functor->free_data = NULL;
    functor->data = config;

    return functor;
}

# endif /* HAVE_PMEMIO */
