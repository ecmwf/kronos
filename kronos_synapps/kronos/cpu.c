/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/**
 * @date May 2016
 * @author Simon Smart
 */

#include <stdlib.h>

#include "kronos/cpu.h"
#include "kronos/global_config.h"
#include "kronos/stats.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_logger("cpu");

    return logger;
}

static TimeSeriesLogger* flops_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("flops");

    return logger;
}

CPUParamsInternal get_cpu_params(const CPUConfig* config) {

    const GlobalConfig* global_conf = global_config_instance();

    CPUParamsInternal params;
    params.node_flops = config->flops / global_conf->nprocs;
    return params;
}


static int execute_cpu(const void* data) {

    const CPUConfig* config = data;
    CPUParamsInternal params;

    long i;
    volatile double c = 0.11, a = 0.5, b = 2.2;

    params = get_cpu_params(config);

    TRACE3("Executing FLOPs: %li, per MPI process: %li", config->flops, params.node_flops);

    stats_start(stats_instance());

    for (i = 0; i < params.node_flops; i++) {
        c += a * b;
    }
    dummy_deoptimise((void*)&c);

    /* Log time series data */

    stats_stop_log(stats_instance(), params.node_flops);
    log_time_series_add_chunk_data(flops_time_series(), params.node_flops);
    log_time_series_chunk();

    return 0;
}


KernelFunctor* init_cpu(const JSON* config_json) {

    CPUConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(CPUConfig));

    if (json_object_get_integer(config_json, "flops", &config->flops) != 0 ||
        config->flops < 0) {

        fprintf(stderr, "Invalid parameters specified in cpu config\n");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_cpu;
    functor->free_data = NULL;
    functor->data = config;

    return functor;
}
