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
#include "kronos/trace.h"




/* ------------------------------------------------------------------------------------------------------------------ */ 

FSMETAParamsInternal get_fsmetadata_params(const FSMETAConfig* config) {

    FSMETAParamsInternal params;

    assert(config->n_mk_dirs > 0);

    /* The mkdir/rmdir are shared out as uniformly as possible across the processors */
    if (config->n_mk_dirs) {
        params.node_n_mk_dirs = global_distribute_work(config->n_mk_dirs);
    }else{
        params.node_n_mk_dirs = 0;
    }

    return params;
}


static long process_dir_counter() {

    static size_t counter = 0;
    return counter++;
}


int get_dir_name(char* path_out, size_t max_len) {

    const GlobalConfig* global_conf = global_config_instance();
    int dir;

    dir = snprintf(path_out, max_len, "%s/%s-%li-%li", global_conf->file_shared_path,
                       global_conf->hostname, (long)getpid(), process_dir_counter());

    return (dir > 0 && dir < PATH_MAX) ? 0 : -1;
}

/**
 * @brief this function creates a directory with the specified path
 */
static bool kronos_make_dir(const char* dir_path) {

    long error;

    error = mkdir(dir_path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if (error){
        fprintf(stderr, "Failed to create directory %s \n", dir_path);
    }

    return error;
}


/**
 * @brief this function removes a directory with the specified path
 */
static bool kronos_rm_dir(const char* dir_path) {

    long error;

    error = rmdir(dir_path);

    if (error){
        fprintf(stderr, "Failed to create directory %s \n", dir_path);
    }

    return error;
}


static int execute_fsmetadata(const void* data) {

    const FSMETAConfig* config = data;
    FSMETAParamsInternal params;

    int error;
    long count;
    bool success;
    char dir_path[PATH_MAX];

    params = get_fsmetadata_params(config);

    TRACE2("Creating/removed %li directories", params.node_n_mk_dirs);


    /* --------- creates/removes directories ----------- */
    error = 0;
    for (count = 0; count < params.node_n_mk_dirs; count++) {

        if (get_dir_name(dir_path, PATH_MAX) == 0) {

            TRACE2("Writing directory %s ...", dir_path);
            error = kronos_make_dir(dir_path);
        }

        if (error) fprintf(stderr, "A write error occurred on creating directory %s\n", dir_path);

        TRACE2("Removing directory %s ...", dir_path);
        error = kronos_rm_dir(dir_path);

        if (error) fprintf(stderr, "A write error occurred on removing directory %s\n", dir_path);

    }

    return error;
}




KernelFunctor* init_fsmetadata(const JSON* config_json) {


    FSMETAConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FSMETAConfig));

    if (json_object_get_integer(config_json, "n_mkdir", &config->n_mk_dirs) != 0 ||
        config->n_mk_dirs < 0) {

        fprintf(stderr, "Invalid parameters specified in fs-metadata config\n");
        free(config);
        return NULL;
    }


    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_fsmetadata;
    functor->data = config;

    return functor;
}















