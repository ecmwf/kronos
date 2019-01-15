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


#ifndef kronos_global_config_H
#define kronos_global_config_H

#include <limits.h>
#include <sys/types.h>
#include <time.h>


#include "kronos/json.h"
#include "kronos/bool.h"

/* ------------------------------------------------------------------------------------------------------------------ */

typedef struct GlobalConfig {

    int nprocs;
    int mpi_rank;

    bool enable_trace;

    char file_read_path[PATH_MAX];
    char file_write_path[PATH_MAX];
    char file_shared_path[PATH_MAX];

    char hostname[HOST_NAME_MAX];
    pid_t pid;
    uid_t uid;

    clock_t start_time;
    time_t start_time2;
    double start_time3;

    bool print_statistics;
    bool write_statistics_file;
    char statistics_file[PATH_MAX];

    long file_read_multiplicity;
    long file_read_size_min_pow;
    long file_read_size_max_pow;

    long file_read_size_min;
    long file_read_size_max;

    long job_num;

    bool enable_notifications;
    char notification_host[HOST_NAME_MAX];
    long notification_port;

} GlobalConfig;


int init_global_config(const JSON* json, int argc, char** argv);
void clean_global_config();

const GlobalConfig* global_config_instance();

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_global_config_H */
