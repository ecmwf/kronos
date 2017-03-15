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
#include <assert.h>

#include "kronos/fs_metadata.h"
#include "kronos/global_config.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */ 

FSMETAParamsInternal get_fsmetadata_params(const FSMETAConfig* config) {

    FSMETAParamsInternal params;

    assert(config->n_mk_dirs > 0);
    assert(config->n_rm_dirs > 0);

    /* The mkdir/rmdir are shared out as uniformly as possible across the processors */
    params.node_n_mk_dirs = global_distribute_work(config->n_mk_dirs);
    params.node_n_rm_dirs = global_distribute_work(config->n_rm_dirs);

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




static int execute_fsmetadata(const void* data) {

    const FSMETAConfig* config = data;
    FSMETAParamsInternal params;

    int error;
    long count;
    bool success;
    char file_path[PATH_MAX];

    params = get_fsmetadata_params(config);

    TRACE3("Creating %li directories and removing %li directories", params.node_n_mk_dirs, params.node_n_rm_dirs);

    /* ----- TODO add instructions here.. -------*/
    TRACE1("... doing nothing for now..");

    error = 0;

    return error;
}


KernelFunctor* init_fsmetadata(const JSON* config_json) {


    FSMETAConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FSMETAConfig));

    if (json_object_get_integer(config_json, "n_mkdir", &config->n_mk_dirs) != 0 ||
        json_object_get_integer(config_json, "n_rmdir", &config->n_rm_dirs) != 0 ||
        config->n_mk_dirs < 0 ||
        config->n_rm_dirs <= 0) {

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















