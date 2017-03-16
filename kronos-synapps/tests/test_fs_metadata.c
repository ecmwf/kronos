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

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "kronos/fs_metadata.h"
#include "kronos/global_config.h"

/* ------------------------------------------------------------------------------------------------------------------ */


static void test_dir_params() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    FSMETAConfig config;
    FSMETAParamsInternal params;

    config.n_mk_dirs = 50;

    reset_global_distribute();
    params = get_fsmetadata_params(&config);

    /* n.b. kilobytes is converted to bytes */

    assert(params.node_n_mk_dirs == 50);

    /* If we change the number of processes, the work should be appropriately distributed. */

    gconfig->nprocs = 5;
    gconfig->mpi_rank = 0;
    reset_global_distribute();
    params = get_fsmetadata_params(&config);

    assert(params.node_n_mk_dirs == 10);

    /* Restore the global config */
    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;
}


int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    printf("# MPI threads: %i\n", global_config_instance()->nprocs);

    test_dir_params();


    clean_global_config();

    return 0;
}
