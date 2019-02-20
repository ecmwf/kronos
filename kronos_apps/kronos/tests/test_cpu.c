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
 * @date Jun 2016
 * @author Simon Smart
 */

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif
#include <stdlib.h>

#include "kronos/global_config.h"
#include "common/json.h"
#include "common/utility.h"
#include "kronos/kernels.h"
#include "kronos/cpu.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void test_cpu_params() {

    /* Test that the workload is divided up amongst the avaialable processes. Don't worry about it being
     * exact (+/- 1 flop) when dividing large numbers. This is well within the error margin of what we can
     * express with a kernel */

    CPUConfig config;
    CPUParamsInternal params;

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    /* Test using the testing default (1 process) */

    config.flops = 12345;
    params = get_cpu_params(&config);
    assert(params.node_flops == 12345);

    /* And try increasing it */

    gconfig->nprocs = 2;
    gconfig->mpi_rank = 0;
    params = get_cpu_params(&config);
    assert(params.node_flops == 6172);

    gconfig->nprocs = 9;
    gconfig->mpi_rank = 4;
    params = get_cpu_params(&config);
    assert(params.node_flops == 1371);

    /* Reset the global config */

    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;
}


static void test_cpu_kernel_init() {

    KernelFunctor *kernel, *kernel2;
    const CPUConfig* config;

    JSON* json = json_from_string("{\"name\": \"cpu\", \"flops\": 12345}");
    assert(json);

    /* Initialise kernel directly, and via the factory */

    kernel = init_cpu(json);
    kernel2 = kernel_factory(json);

    assert(kernel != NULL);
    assert(kernel->data != NULL);
    assert(kernel->execute != NULL);
    assert(kernel2 != NULL);
    assert(kernel2->data != NULL);
    assert(kernel2->execute != NULL);

    /* Test that the factory has initialised the same kernel as the direct method */

    assert(kernel->execute == kernel2->execute);

    /* Check the initialised params */

    config = (const CPUConfig*)kernel->data;
    assert(config->flops == 12345);

    config = (const CPUConfig*)kernel2->data;
    assert(config->flops == 12345);

    free_kernel(kernel);
    free_kernel(kernel2);
    free_json(json);

    /* Test what happens if we don't provide the required fields */

    json = json_from_string("{\"name\": \"cpu\"}");
    assert(json);
    kernel = init_cpu(json);
    assert(kernel == NULL);
    free_json(json);

    /* And invalid values */

    json = json_from_string("{\"name\": \"cpu\", \"flops\": \"invalid\"}");
    assert(json);
    kernel = init_cpu(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"cpu\", \"flops\": -1}");
    assert(json);
    kernel = init_cpu(json);
    assert(kernel == NULL);
    free_json(json);
}


/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    test_cpu_params();
    test_cpu_kernel_init();

    clean_global_config();
    return 0;
}
