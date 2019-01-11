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
#include <sys/types.h>
#include <sys/stat.h>

#include "kronos/configure_read_files.h"
#include "kronos/global_config.h"
#include "kronos/kronos_version.h"
#include "kronos/mpi_kernel.h"
#include "kronos/utility.h"


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

    if ((tmp_json = json_object_get(json, "file_shared_path")) != NULL) {
        if (json_as_string(tmp_json, global_config.file_shared_path, PATH_MAX) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_shared_path as string from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    } else {
        getcwd(global_config.file_shared_path, PATH_MAX);
        strcat(global_config.file_shared_path, "/shared");
    }

    if ((tmp_json = json_object_get(json, "statistics_path")) != NULL) {
        if (json_as_string(tmp_json, global_config.statistics_file, PATH_MAX) != 0) {
            error = -1;
            fprintf(stderr, "Error reading statistics_path as string from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    } else {
        getcwd(global_config.statistics_file, PATH_MAX);
        strcat(global_config.statistics_file, "/statistics.kresults");
    }

    /* The above paths can be overridden from environment variables */

    env_ptr = getenv("KRONOS_WRITE_DIR");
    if (env_ptr != NULL)
        strncpy(global_config.file_write_path, env_ptr, PATH_MAX);

    env_ptr = getenv("KRONOS_READ_DIR");
    if (env_ptr != NULL)
        strncpy(global_config.file_read_path, env_ptr, PATH_MAX);

    env_ptr = getenv("KRONOS_SHARED_DIR");
    if (env_ptr != NULL)
        strncpy(global_config.file_shared_path, env_ptr, PATH_MAX);

    env_ptr = getenv("KRONOS_STATS_FILE");
    if (env_ptr != NULL)
        strncpy(global_config.statistics_file, env_ptr, PATH_MAX);

    /*
     * Behaviour of the write kernel
     */

    /* Set up the umask */

    umask(0027);

    global_config.file_read_multiplicity = file_read_multiplicity;
    if ((tmp_json = json_object_get(json, "file_read_multiplicity")) != NULL) {
        if (json_as_integer(tmp_json, &global_config.file_read_multiplicity) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_read_multiplicity as integer from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    global_config.file_read_size_min_pow = file_read_size_min_pow;
    if ((tmp_json = json_object_get(json, "file_read_size_min_pow")) != NULL) {
        if (json_as_integer(tmp_json, &global_config.file_read_size_min_pow) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_read_size_min_pow as integer from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    global_config.file_read_size_max_pow = file_read_size_max_pow;
    if ((tmp_json = json_object_get(json, "file_read_size_max_pow")) != NULL) {
        if (json_as_integer(tmp_json, &global_config.file_read_size_max_pow) != 0) {
            error = -1;
            fprintf(stderr, "Error reading file_read_size_max_pow as integer from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    global_config.file_read_size_min = 1 << global_config.file_read_size_min_pow;
    global_config.file_read_size_max = 1 << global_config.file_read_size_max_pow;

    /*
     * Control statistics output
     */

    global_config.print_statistics = false;
    if ((tmp_json = json_object_get(json, "print_statistics")) != NULL) {
        if (json_as_boolean(tmp_json, &global_config.print_statistics) != 0) {
            error = -1;
            fprintf(stderr, "Error reading print_statistics as boolean from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    global_config.write_statistics_file = true;
    if ((tmp_json = json_object_get(json, "write_statistics_file")) != NULL) {
        if (json_as_boolean(tmp_json, &global_config.write_statistics_file) != 0) {
            error = -1;
            fprintf(stderr, "Error reading write_statistics_file as boolean from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

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
    global_config.pid = getpid();
    global_config.uid = getuid();

    global_config.start_time = clock();
    global_config.start_time2 = time(NULL);
    global_config.start_time3 = take_time();

    /*
     * Host to send notification JSONs to
     */

    global_config.notification_port = 7363;
    global_config.enable_notifications = false;
    global_config.job_num = 0;

    if ((tmp_json = json_object_get(json, "notification_host")) != NULL) {
        if (json_as_string(tmp_json, global_config.notification_host, HOST_NAME_MAX) != 0) {
            error = -1;
            fprintf(stderr, "Error reading notification_host as string from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        } else {
            global_config.enable_notifications = true;
        }
    }

    if ((tmp_json = json_object_get(json, "notification_port")) != NULL) {
        if (json_as_integer(tmp_json, &global_config.notification_port) != 0) {
            error = -1;
            fprintf(stderr, "Error reading notification_port as integer from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

    if ((tmp_json = json_object_get(json, "job_num")) != NULL) {
        if (json_as_integer(tmp_json, &global_config.job_num) != 0) {
            error = -1;
            fprintf(stderr, "Error reading job_num as integer from global configuration\nGot:");
            print_json(stderr, tmp_json);
            fprintf(stderr, "\n");
        }
    }

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
        printf("Kronos version: %s\n", kronos_version_str());
        printf("Kronos git SHA1: %s\n", kronos_git_sha1());
        printf("File read cache: %s\n", global_config.file_read_path);
        printf("File write cache: %s\n", global_config.file_write_path);
        printf("File shared path: %s\n", global_config.file_shared_path);
        printf("Read cache multiplicity: %ld\n", global_config.file_read_multiplicity);
        printf("Read cache minimum file size: %ld bytes\n", global_config.file_read_size_min);
        printf("Read cache maximum file size: %ld bytes\n", global_config.file_read_size_max);
        if (global_config.write_statistics_file)
            printf("Statistics output file: %s\n", global_config.statistics_file);
        printf("Hostname: %s\n", global_config.hostname);
        printf("Trace output: %s\n", (global_config.enable_trace ? "true":"false"));
        printf("Print statistics: %s\n", (global_config.print_statistics ? "true":"false"));
        printf("Finalisation status notification: %s\n", global_config.enable_notifications ? "enabled" : "disabled");
        if (global_config.enable_notifications) {
            printf("Notification hostname: %s\n", global_config.notification_host);
            printf("Notification port: %li\n", global_config.notification_port);
        }
        printf("Job ID number: %li\n", global_config.job_num);
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
