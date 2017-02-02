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
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <errno.h>

#include "kronos/configure_read_files.h"
#include "kronos/file_read.h"
#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

FileReadParamsInternal get_read_params(const FileReadConfig* config) {


    long read_size, total_size;
    FileReadParamsInternal params;

    assert(config->reads > 0);

    params.num_reads = global_distribute_work(config->reads);

    /* The average read size, which is then stochastically rounded up/down for each of the actual reads to get the
     * correct overall behaviour */

    /* n.b. Convert kb to bytes */
    read_size = 1024 * config->kilobytes / config->reads;
    params.power_of_2 = is_power_of_2(read_size);

    if (!params.power_of_2) {
        params.larger_size = next_power_of_2(read_size);
        params.smaller_size = params.larger_size >> 1;
        params.prob_small = ((double)(read_size - params.smaller_size))
                          / ((double)(params.larger_size - params.smaller_size));;
    } else {
        params.smaller_size = read_size;
        params.larger_size = read_size;
    }

    /* Deal with edge cases! */

    if (params.smaller_size < file_read_size_min) {
        params.smaller_size = file_read_size_min;
        params.larger_size = file_read_size_min;
        params.power_of_2 = true;
    }

    if (params.larger_size > file_read_size_max) {
        params.power_of_2 = true;

        total_size = params.num_reads * read_size;
        params.num_reads = 1 + ((total_size - 1) / file_read_size_max);

        params.larger_size = file_read_size_max;
        params.smaller_size = file_read_size_max;
    }

    return params;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and read
 *        the data into a buffer using the mmap procedure
 */
static bool file_read_mmap(const char* file_path, long read_size, char* buffer) {

    int fd;
    char* pmapped;
    bool success;

    TRACE();

    success = false;

    fd = open(file_path, O_RDONLY);
    if (fd != -1) {

        pmapped = mmap(NULL, read_size, PROT_READ, MAP_PRIVATE, fd, 0);
        /* pmapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE | MAP_FILE, fd, 0); */

        if (pmapped != MAP_FAILED) {
            success = true;
            memcpy(buffer, pmapped, read_size);
            munmap(pmapped, read_size);
        } else {
            fprintf(stderr, "mmap error: (%d) %s\n", errno, strerror(errno));
        }
        close(fd);
    }

    return success;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and read
 *        the data into a buffer using the standard C api
 * @return
 */
static bool file_read_c_api(const char* file_path, long read_size, char* buffer) {

    FILE* file;
    bool success;
    long result;

    TRACE();

    success = false;

    file = fopen(file_path, "rb");
    if (file != NULL) {

        result = fread(buffer, 1, read_size, file);

        if (result != read_size) {
            fprintf(stderr, "A read error occurred reading file: %s (%d) %s\n", file_path, errno, strerror(errno));
        } else {
            success = true;
        }
        fclose(file);
    }

    return success;

}


int execute_file_read(const void* data) {

    const FileReadConfig* config = data;
    const GlobalConfig* global_conf = global_config_instance();

    long size;
    int count, file_index, written, error;
    bool success;
    char file_path[PATH_MAX];
    char * read_buffer;

    FileReadParamsInternal params = get_read_params(config);

    if (params.power_of_2)
        TRACE3("Read %li files, %li bytes each.", params.num_reads, params.larger_size);
    else
        TRACE4("Read %li files, %li - %li bytes each.", params.num_reads, params.smaller_size, params.larger_size);

    /* And do the actual reading!!! */

    read_buffer = malloc(params.larger_size);

    error = 0;
    for (count = 0; count < params.num_reads; count++) {

        if (!params.power_of_2 && (double)rand() / (double)RAND_MAX < params.prob_small) {
            size = params.smaller_size;
        } else {
            size = params.larger_size;
        }

        /* Pick a random file of the correct size */
        file_index = rand() % file_read_multiplicity;

        success = false;
        written = snprintf(file_path, PATH_MAX, "%s/read-cache-%li-%d", global_conf->file_read_path, size, file_index);
        if (written > 0 && written < PATH_MAX) {

            TRACE2("Reading from file %s ...", file_path);

            if (config->mmap) {
                success = file_read_mmap(file_path, size, read_buffer);
            } else {
                success = file_read_c_api(file_path, size, read_buffer);
            }

            TRACE1("... done");
        }

        /* And some error handling */

        if (!success) {
            fprintf(stderr, "A read error occurred on file %s\n", file_path);
            error = -1;
        }
    }

    free(read_buffer);

    return error;
}


KernelFunctor* init_file_read(const JSON* config_json) {

    FileReadConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FileReadConfig));

    if (json_object_get_integer(config_json, "kb_read", &config->kilobytes) != 0 ||
        json_object_get_integer(config_json, "n_read", &config->reads) != 0 ||
        config->kilobytes < 0 ||
        config->reads <= 0) {

        fprintf(stderr, "Invalid parameters specified in file-read config\n");
        free(config);
        return NULL;
    }

    if (json_object_get_boolean(config_json, "mmap", &config->mmap) != 0) {
        config->mmap = false;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_file_read;
    functor->data = config;

    return functor;
}