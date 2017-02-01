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

#include "kronos/configure_read_files.h"
#include "kronos/file_read.h"
#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void test_global_distribute() {

    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 0;

    reset_global_distribute();
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 2);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 2);

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 1;

    reset_global_distribute();
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 2);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 2);
    assert(global_distribute_work(8) == 3);

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 2;

    reset_global_distribute();
    assert(global_distribute_work(8) == 2);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 2);
    assert(global_distribute_work(8) == 3);
    assert(global_distribute_work(8) == 3);

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 0;

    reset_global_distribute();
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);
    gconfig->mpi_rank = 1;
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);
    gconfig->mpi_rank = 2;
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);
    assert(global_distribute_work(6) == 2);

    /* Safely restore the global config */

    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;
    reset_global_distribute();
}

static void test_valid_size_limits() {

    assert(is_power_of_2(file_read_size_min));
    assert(is_power_of_2(file_read_size_max));

    assert(file_read_size_min < file_read_size_max);
    assert(file_read_size_min_pow < file_read_size_max_pow);
    assert(file_read_size_min_pow > 0);
    assert(file_read_size_max_pow > 0);
}


static void test_read_params_rounding() {

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 1;
    config.kilobytes = 20000;

    params = get_read_params(&config);

    assert(params.smaller_size == 0x1000000);
    assert(params.larger_size == 0x2000000);
    assert(!params.power_of_2);
    assert(params.num_reads == 1);
    assert(fabs(params.prob_small - 0.220703125) < 1e-8);
}


static void test_read_params_power_of_2() {

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 1;
    config.kilobytes = 16384;

    params = get_read_params(&config);

    assert(params.smaller_size == 0x1000000);
    assert(params.larger_size == 0x1000000);
    assert(params.power_of_2);
    assert(params.num_reads == 1);
}


static void test_read_params_too_small() {

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 1;
    config.kilobytes = 1;

    params = get_read_params(&config);

    assert(params.smaller_size == file_read_size_min);
    assert(params.larger_size == file_read_size_min);
    assert(params.power_of_2);
    assert(params.num_reads == 1);


    config.reads = 3;
    config.kilobytes = file_read_size_min/1024;
    params = get_read_params(&config);

    assert(params.smaller_size == file_read_size_min);
    assert(params.larger_size == file_read_size_min);
    assert(params.power_of_2);
    assert(params.num_reads == 3);
}


static void test_read_params_too_big() {

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 2;
    config.kilobytes = 3 * file_read_size_max / 1024;

    params = get_read_params(&config);

    assert(params.smaller_size == file_read_size_max);
    assert(params.larger_size == file_read_size_max);
    assert(params.power_of_2);
    assert(params.num_reads == 3);
}


static void test_read_params_num_reads() {

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 1;
    config.kilobytes = 0x10000;

    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 1);
    assert(params.larger_size == 0x4000000);
    assert(params.power_of_2);

    config.reads = 2;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 2);
    assert(params.larger_size == 0x2000000);
    assert(params.power_of_2);

    config.reads = 3;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 3);
    assert(params.larger_size == 0x2000000);
    assert(params.smaller_size == 0x1000000);
    assert(!params.power_of_2);

    config.reads = 7;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 7);
    assert(params.larger_size == 0x1000000);
    assert(params.smaller_size == 0x800000);
    assert(!params.power_of_2);
}


static void test_read_params_mpi_threads() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    FileReadConfig config;
    FileReadParamsInternal params;

    config.reads = 8;
    config.kilobytes = 0x40000;

    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 8);
    assert(params.larger_size == 0x2000000);
    assert(params.power_of_2);

    /* Reads split between the ranks. N.b. Test that non-uniform splitting is done correctly */

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 1;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 3);
    assert(params.larger_size == 0x2000000);
    assert(params.power_of_2);

    gconfig->nprocs = 3;
    gconfig->mpi_rank = 2;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 2);
    assert(params.larger_size == 0x2000000);
    assert(params.power_of_2);

    /* Test that this works correctly with the larger/smaller partitioning */

    config.reads = 7;
    gconfig->nprocs = 3;
    gconfig->mpi_rank = 1;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 2);
    assert(params.larger_size == 0x4000000);
    assert(params.smaller_size == 0x2000000);
    assert(!params.power_of_2);

    /* Test that we still catch the too-small case */

    config.reads = 7;
    config.kilobytes = file_read_size_min * 2 / 1024;
    gconfig->nprocs = 3;
    gconfig->mpi_rank = 1;
    reset_global_distribute();
    params = get_read_params(&config);

    assert(params.num_reads == 2);
    assert(params.larger_size == file_read_size_min);
    assert(params.smaller_size == file_read_size_min);
    assert(params.power_of_2);

    /* Safely restore the global config */
    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;

}


static void test_read_kernel_init() {

    const FileReadConfig* config;
    KernelFunctor *kernel, *kernel2;

    JSON* json = json_from_string("{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3}");
    assert(json);
    kernel = init_file_read(json);

    assert(kernel != NULL);
    assert(kernel->data != NULL);
    assert(kernel->execute != NULL);

    config = (const FileReadConfig*)kernel->data;
    assert(config->kilobytes == 1234);
    assert(config->reads == 3);
    assert(config->mmap == false);

    /* Test that the global kernel factory gives the correct kernel! */

    kernel2 = kernel_factory(json);
    assert(kernel2->execute == kernel->execute);

    free_kernel(kernel);
    free_kernel(kernel2);
    free_json(json);

    /* Test that we can modify any optional fields */

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3, \"mmap\": true}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel != NULL);
    config = (const FileReadConfig*)kernel->data;
    assert(config->mmap == true);
    free_json(json);

    /* Test what happens if we don't provide the required fields */

    json = json_from_string("{\"name\": \"file-read\", \"n_read\": 3}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": 1234}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);

    /* And invalid parameters? */

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": \"invalid\", \"n_read\": 3}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": \"invalid\"}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": -1234, \"n_read\": 3}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": -3}");
    assert(json);
    kernel = init_file_read(json);
    assert(kernel == NULL);
    free_json(json);



}

/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    test_global_distribute();
    test_valid_size_limits();
    test_read_params_rounding();
    test_read_params_power_of_2();
    test_read_params_too_small();
    test_read_params_too_big();
    test_read_params_num_reads();
    test_read_params_mpi_threads();
    test_read_kernel_init();

    clean_global_config();
    return 0;
}
