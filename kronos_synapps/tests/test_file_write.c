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

#include "kronos/file_write.h"
#include "kronos/global_config.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void test_write_params() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    FileWriteConfig config;
    FileWriteParamsInternal params;

    config.writes = 7;
    config.kilobytes = 0x70000;

    reset_global_distribute();
    params = get_write_params(&config);

    /* n.b. kilobytes is converted to bytes */

    assert(params.num_writes == 7);
    assert(params.write_size == 0x4000000);

    /* If we change the number of processes, the work should be appropriately distributed. */

    gconfig->nprocs = 4;
    gconfig->mpi_rank = 0;
    reset_global_distribute();
    params = get_write_params(&config);

    assert(params.num_writes == 2);
    assert(params.write_size == 0x4000000);

    gconfig->nprocs = 4;
    gconfig->mpi_rank = 2;
    reset_global_distribute();
    params = get_write_params(&config);

    assert(params.num_writes == 2);
    assert(params.write_size == 0x4000000);

    gconfig->nprocs = 4;
    gconfig->mpi_rank = 3;
    reset_global_distribute();
    params = get_write_params(&config);

    assert(params.num_writes == 1);
    assert(params.write_size == 0x4000000);

    /* Restore the global config */
    gconfig->nprocs = 1;
    gconfig->mpi_rank = 0;
}

static void test_write_filename() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();

    char path_storage[PATH_MAX], host_storage[PATH_MAX], path_tmp[PATH_MAX], path_out[PATH_MAX];
    const char* tmpdir;
    int test_val; /* Used to avoid c89 warnings with GCC */

    tmpdir = getenv("TMPDIR");
    assert(tmpdir != NULL);

    /* Switch our own path into place */
    strncpy(path_storage, gconfig->file_write_path, sizeof(gconfig->file_write_path));
    strncpy(gconfig->file_write_path, tmpdir, sizeof(gconfig->file_write_path));

    strncpy(host_storage, gconfig->hostname, sizeof(gconfig->hostname));
    strncpy(gconfig->hostname, "HOST", 5);

    /* What do we expect the pathname to be? */
    get_file_write_name(path_out, PATH_MAX);
    snprintf(path_tmp, PATH_MAX, "%s/HOST-%li-0", tmpdir, (long)getpid());
    test_val = strncmp(path_tmp, path_out, PATH_MAX);
    assert(test_val == 0);

    get_file_write_name(path_out, PATH_MAX);
    snprintf(path_tmp, PATH_MAX, "%s/HOST-%li-1", tmpdir, (long)getpid());
    test_val = strncmp(path_tmp, path_out, PATH_MAX);
    assert(test_val == 0);

    /* Restore the global config */
    strcpy(gconfig->file_write_path, path_storage);
    strncpy(gconfig->hostname, host_storage, sizeof(gconfig->hostname));

}


static void test_write_kernel_init() {

    const FileWriteConfig* config;
    KernelFunctor *kernel, *kernel2;

    JSON* json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1234, \"n_write\": 3}");
    assert(json);
    kernel = init_file_write(json);

    assert(kernel != NULL);
    assert(kernel->data != NULL);
    assert(kernel->execute != NULL);

    config = (const FileWriteConfig*)kernel->data;
    assert(config->kilobytes == 1234);
    assert(config->writes == 3);
    assert(config->mmap == false);

    /* Test that the global kernel factory gives the correct kernel! */

    kernel2 = kernel_factory(json);
    assert(kernel2->execute == kernel->execute);

    free_kernel(kernel);
    free_kernel(kernel2);
    free_json(json);

    /* Test that we can modify the optional fields */

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1234, \"n_write\": 3, \"mmap\": true}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel != NULL);
    config = (const FileWriteConfig*)kernel->data;
    assert(config->mmap == true);
    free_json(json);

    /* Test what happens if we don't provide the required fields */

    json = json_from_string("{\"name\": \"file-write\", \"n_write\": 3}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1234}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);

    /* And invalid parameters? */

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": \"invalid\", \"n_write\": 3}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1234, \"n_write\": \"invalid\"}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": -1234, \"n_write\": 3}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1234, \"n_write\": -3}");
    assert(json);
    kernel = init_file_write(json);
    assert(kernel == NULL);
    free_json(json);



}

static void test_write_file() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();
    KernelFunctor* kernel;
    JSON* json;
    FILE* file;

    char path_storage[PATH_MAX], host_storage[PATH_MAX], path_tmp[PATH_MAX];
    const char* tmpdir;

    tmpdir = getenv("TMPDIR");
    assert(tmpdir != NULL);

    /* Switch our own path into place */
    strncpy(path_storage, gconfig->file_write_path, sizeof(gconfig->file_write_path));
    strncpy(gconfig->file_write_path, tmpdir, sizeof(gconfig->file_write_path));

    strncpy(host_storage, gconfig->hostname, sizeof(gconfig->hostname));
    strncpy(gconfig->hostname, "HOST", 5);

    /* Initialise a kernel to do some writing, and execute it */

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1000, \"n_write\": 2, \"mmap\": false}");
    assert(json);

    kernel = kernel_factory(json);
    assert(kernel);

    kernel->execute(kernel->data);

    free_kernel(kernel);
    free_json(json);

    /* Test that the appropriate files have been created (and clean them up)
     * n.b. the file number sequence continues from the other tests */

    snprintf(path_tmp, PATH_MAX, "%s/%s-%li-2", tmpdir, gconfig->hostname, (long)getpid());

    assert(access(path_tmp, F_OK) != -1);
    file = fopen(path_tmp, "r");
    assert(file != NULL);
    fseek(file, 0L, SEEK_END);
    assert(ftell(file) == 500 * 1024);
    fclose(file);

    remove(path_tmp);

    snprintf(path_tmp, PATH_MAX, "%s/%s-%li-3", tmpdir, gconfig->hostname, (long)getpid());

    assert(access(path_tmp, F_OK) != -1);
    file = fopen(path_tmp, "r");
    assert(file != NULL);
    fseek(file, 0L, SEEK_END);
    assert(ftell(file) == 500 * 1024);
    fclose(file);

    remove(path_tmp);

    /* Restore the global config */
    strcpy(gconfig->file_write_path, path_storage);
    strncpy(gconfig->hostname, host_storage, sizeof(gconfig->hostname));
}


static void test_write_file_mmap() {

    /* Const cast, so we can manipulate the global config and make it look like we have multiple procs. */
    GlobalConfig* gconfig = (GlobalConfig*)global_config_instance();
    KernelFunctor* kernel;
    JSON* json;
    FILE* file;

    char path_storage[PATH_MAX], host_storage[PATH_MAX], path_tmp[PATH_MAX];
    const char* tmpdir;

    tmpdir = getenv("TMPDIR");
    assert(tmpdir != NULL);

    /* Switch our own path into place */
    strncpy(path_storage, gconfig->file_write_path, sizeof(gconfig->file_write_path));
    strncpy(gconfig->file_write_path, tmpdir, sizeof(gconfig->file_write_path));

    strncpy(host_storage, gconfig->hostname, sizeof(gconfig->hostname));
    strncpy(gconfig->hostname, "HOST", 5);

    /* Initialise a kernel to do some writing, and execute it */

    json = json_from_string("{\"name\": \"file-write\", \"kb_write\": 1000, \"n_write\": 2, \"mmap\": true}");
    assert(json);

    kernel = kernel_factory(json);
    assert(kernel);

    kernel->execute(kernel->data);

    free_kernel(kernel);
    free_json(json);

    /* Test that the appropriate files have been created (and clean them up)
     * n.b. the file number sequence continues from the other tests */

    snprintf(path_tmp, PATH_MAX, "%s/%s-%li-4", tmpdir, gconfig->hostname, (long)getpid());

    assert(access(path_tmp, F_OK) != -1);
    file = fopen(path_tmp, "r");
    assert(file != NULL);
    fseek(file, 0L, SEEK_END);
    assert(ftell(file) == 500 * 1024);
    fclose(file);

    remove(path_tmp);

    snprintf(path_tmp, PATH_MAX, "%s/%s-%li-5", tmpdir, gconfig->hostname, (long)getpid());

    assert(access(path_tmp, F_OK) != -1);
    file = fopen(path_tmp, "r");
    assert(file != NULL);
    fseek(file, 0L, SEEK_END);
    assert(ftell(file) == 500 * 1024);
    fclose(file);

    remove(path_tmp);

    /* Restore the global config */
    strcpy(gconfig->file_write_path, path_storage);
    strncpy(gconfig->hostname, host_storage, sizeof(gconfig->hostname));
}

/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    test_write_params();
    test_write_filename();
    test_write_kernel_init();
    test_write_file();
    test_write_file_mmap();

    clean_global_config();
    return 0;
}
