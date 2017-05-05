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

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "kronos/fs_metadata.h"
#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/stats.h"
#include "kronos/trace.h"

/* ------------------------------------------------------------------------------------------------------------------ */ 


static StatisticsLogger* mkdir_stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_logger("mkdir");

    return logger;
}


static StatisticsLogger* rmdir_stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_logger("rmdir");

    return logger;
}

FsMetadataParamsInternal get_fsmetadata_params(const FsMetadataConfig* config) {

    FsMetadataParamsInternal params;

    assert(config->n_mkdirs > 0);

    /* The mkdir/rmdir are shared out as uniformly as possible across the processors */
    if (config->n_mkdirs) {
        params.node_n_mkdirs = global_distribute_work(config->n_mkdirs);
    } else {
        params.node_n_mkdirs = 0;
    }

    return params;
}


static long process_dir_counter() {

    static size_t counter = 0;
    return counter++;
}


int get_dir_name(char* path_out, size_t max_len) {

    const GlobalConfig* global_conf = global_config_instance();
    int len;

    len = snprintf(path_out, max_len, "%s/%s-%li-%li", global_conf->file_shared_path,
                       global_conf->hostname, (long)getpid(), process_dir_counter());

    return (len > 0 && len < PATH_MAX) ? 0 : -1;
}

/**
 * @brief this function creates a directory with the specified path
 */
static bool kronos_mkdir(const char* dir_path) {

    bool success = true;

    stats_start(rmdir_stats_instance());
    if (mkdir(dir_path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP) != 0) {
        fprintf(stderr, "Failed to create directory %s (%s)\n", dir_path, strerror(errno));
        success = false;
    }
    stats_stop_log(rmdir_stats_instance(), 1);

    return success;
}


/**
 * @brief this function removes a directory with the specified path
 */
static bool kronos_rmdir(const char* dir_path) {

    bool success = true;

    stats_start(mkdir_stats_instance());
    if (rmdir(dir_path) != 0) {
        fprintf(stderr, "Failed to remove directory %s (%s)\n", dir_path, strerror(errno));
        success = false;
    }
    stats_stop_log(mkdir_stats_instance(), 1);

    return success;
}


static int execute_fsmetadata(const void* data) {

    const FsMetadataConfig* config = data;
    FsMetadataParamsInternal params;

    int error;
    long count;
    bool success;
    char dir_path[PATH_MAX];

    params = get_fsmetadata_params(config);

    TRACE2("Creating/removed %li directories", params.node_n_mkdirs);


    /* --------- creates/removes directories ----------- */
    error = 0;
    for (count = 0; count < params.node_n_mkdirs; count++) {

        success = false;

        if (get_dir_name(dir_path, PATH_MAX) == 0) {

            TRACE2("Writing directory %s", dir_path);
            success = kronos_mkdir(dir_path);

            if (success) {

                TRACE2("Removing directory %s", dir_path);
                success = kronos_rmdir(dir_path);
            }
        }

        if (!success) {
            fprintf(stderr, "An error occurred in create/delete directory %s\n", dir_path);
            error = -1;
        }
    }

    return error;
}




KernelFunctor* init_fsmetadata(const JSON* config_json) {


    FsMetadataConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FsMetadataConfig));

    if (json_object_get_integer(config_json, "n_mkdir", &config->n_mkdirs) != 0 ||
        config->n_mkdirs < 0) {

        fprintf(stderr, "Invalid directory count specified in fs-metadata config\n");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_fsmetadata;
    functor->data = config;

    return functor;
}















