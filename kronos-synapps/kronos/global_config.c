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
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "kronos/global_config.h"
#include "kronos/mpi.h"


static GlobalConfig global_config;

/* ------------------------------------------------------------------------------------------------------------------ */

int init_global_config(const JSON* json, int argc, char** argv) {

    const JSON* tmp_json;
    int error = 0;
    int mpi_procs;
    bool be_verbose;
    long nprocs;
    const char* env_ptr;

    /* Initialise MPI if, and only if, it is available */

#ifdef HAVE_MPI
    error = MPI_Init(&argc, &argv);
#else
    error = 0;
#endif

    /*
     * Read and write paths for file I/O
     */

    if ((tmp_json = json_object_get(json, "file_read_path")) != NULL) {
        if (json_as_string(tmp_json, global_config.file_read_path, PATH_MAX) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_read_path as string from global configuration\n Got:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    } else {
        getcwd(global_config.file_read_path, PATH_MAX);
        strcat(global_config.file_read_path, "/read_cache");
    }

    if ((tmp_json = json_object_get(json, "file_write_path")) != NULL) {
        if (json_as_string(tmp_json, global_config.file_write_path, PATH_MAX) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_write_path as string from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    } else {
        getcwd(global_config.file_write_path, PATH_MAX);
        strcat(global_config.file_write_path, "/write_cache");
    }

    /* The above paths can be overridden from environment variables */

    env_ptr = getenv("KRONOS_WRITE_DIR");
    if (env_ptr != NULL)
        strncpy(global_config.file_write_path, env_ptr, PATH_MAX);

    env_ptr = getenv("KRONOS_READ_DIR");
    if (env_ptr != NULL)
        strncpy(global_config.file_read_path, env_ptr, PATH_MAX);

    /*
     * Input conditions
     */

    global_config.enable_trace = false;
    if ((tmp_json = json_object_get(json, "enable_trace")) != NULL) {
        if (json_as_boolean(tmp_json, &global_config.enable_trace) != 0) {
            error = -1;
            fprintf(stderr, "Error reading enable_trace as boolean from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    /*
     * Current host
     */

    if (gethostname(global_config.hostname, sizeof(global_config.hostname)) != 0) {
        fprintf(stderr, "Error getting current hostname: (%d) %s\n", errno, strerror(errno));
        error = -1;
    }

    global_config.start_time = clock();
    global_config.start_time2 = time(NULL);

    /*
     * Processes and MPI
     */

#ifdef HAVE_MPI
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &global_config.mpi_rank);
    assert(0 <= global_config.mpi_rank && mpi_procs > global_config.mpi_rank);
#else
    mpi_procs = 1;
    global_config.mpi_rank = 0;
#endif

    if ((tmp_json = json_object_get(json, "num_procs")) != NULL) {
        if (json_as_integer(tmp_json, &nprocs) != 0) {
            error = -1;
            fprintf(stderr, "Error reading num_procs from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        } else if (nprocs != mpi_procs) {
            error = -1;
            fprintf(stderr, "Number of MPI processes (%d) does not match global config (%li)\n",
                    mpi_procs, nprocs);
        } else {
            global_config.nprocs = nprocs;
        }
    } else {
        global_config.nprocs = mpi_procs;
    }

    be_verbose = false;
    if ((tmp_json = json_object_get(json, "mpi_ranks_verbose")) != NULL) {
        if (json_as_boolean(tmp_json, &be_verbose) != 0) {
            error = -1;
            fprintf(stderr, "Error reading mpi_ranks_verbose from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

#ifdef HAVE_MPI
    if (!be_verbose && global_config.mpi_rank != 0) {
        freopen("/dev/null", "w", stdout);
    }
#endif

    /*
     * Summary of information
     */

    if (error == 0) {
        printf("Global configuration\n");
        printf("--------------------\n");
        printf("File read cache: %s\n", global_config.file_read_path);
        printf("File write cache: %s\n", global_config.file_write_path);
        printf("Hostname: %s\n", global_config.hostname);
        printf("Trace output: %s\n", (global_config.enable_trace ? "true":"false"));
        printf("Initial timestamp: %f, %s\n",
               (double)global_config.start_time, asctime(localtime(&global_config.start_time)));
#ifdef HAVE_MPI
        printf("MPI processes: %d\n", global_config.nprocs);
        printf("MPI rank: %d\n", global_config.mpi_rank);
#endif
        printf("--------------------\n");
    }

    return error;
}


void clean_global_config() {

#ifdef HAVE_MPI
    MPI_Finalize();
#endif

}


const GlobalConfig* global_config_instance() {

    return &global_config;
}


/* ------------------------------------------------------------------------------------------------------------------ */
