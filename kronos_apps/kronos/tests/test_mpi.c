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
#include "kronos/mpi_kernel.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void test_mpi_params() {

    /* Test that the workload is divided up amongst the avaialable processes. Don't worry about it being
     * exact (+/- 1 flop) when dividing large numbers. This is well within the error margin of what we can
     * express with a kernel */

    MPIConfig config;
    MPIParamsInternal params;

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    config.ncollective = 7;
    config.kb_collective = 0x7000;
    config.npairwise = 5;
    config.kb_pairwise = 0xa000;
    params = get_mpi_params(&config);

    assert(params.nbroadcast == 7);
    assert(params.broadcast_size == 0x400000);
    assert(params.npairwise == 5);
    assert(params.pairwise_size == 0x800000);

    /* These should be independent of the number of MPI processes */

    gconfig->nprocs = 8;
    gconfig->mpi_rank = 3;
    params = get_mpi_params(&config);

    assert(params.nbroadcast == 7);
    assert(params.broadcast_size == 0x400000);
    assert(params.npairwise == 5);
    assert(params.pairwise_size == 0x800000);

    /* Should deal cleanly with zero values */

    config.ncollective = 0;
    config.npairwise = 0;
    params = get_mpi_params(&config);

    assert(params.nbroadcast == 0);
    assert(params.broadcast_size == 0);
    assert(params.npairwise == 0);
    assert(params.pairwise_size == 0);

    /* Reset the global config */

    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;
}


static void test_mpi_kernel_init() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    KernelFunctor *kernel, *kernel2;
    const MPIConfig* config;

    JSON* json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);

    /* Set the number of MPI processes to look > 2, so that we can test npairwise */

    gconfig->nprocs = 4;
    gconfig->mpi_rank = 1;

    /* Initialise kernel directly, and via the factory */

    kernel = init_mpi(json);
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

    config = (const MPIConfig*)kernel->data;
    assert(config->kb_collective == 12345);
    assert(config->ncollective == 6);
    assert(config->kb_pairwise == 54321);
    assert(config->npairwise == 3);

    config = (const MPIConfig*)kernel2->data;
    assert(config->kb_collective == 12345);
    assert(config->ncollective == 6);
    assert(config->kb_pairwise == 54321);
    assert(config->npairwise == 3);

    free_kernel(kernel);
    free_kernel(kernel2);
    free_json(json);

    /* What happens if required fields are missing */

    json = json_from_string("{\"name\": \"mpi\", \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": 54321}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    /* And invalid values */

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": \"invalid\", \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": \"invalid\", \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": \"invalid\", \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": \"invalid\"}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": -1, \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": -1, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": -1, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": -1}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    /* We shouldn't be able to specify non-zero data volumes with zero collective/p2p events */

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 0, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 0, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);

    /* Reset the MPI configuration. Test that we cannot use pairwise MPI with fewer than 2 nodes */

    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;

    json = json_from_string("{\"name\": \"mpi\", \"kb_collective\": 12345, \"n_collective\": 6, \"kb_pairwise\": 54321, \"n_pairwise\": 3}");
    assert(json);
    kernel = init_mpi(json);
    assert(kernel == NULL);
    free_json(json);
}


/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    test_mpi_params();
    test_mpi_kernel_init();

    clean_global_config();
    return 0;
}
