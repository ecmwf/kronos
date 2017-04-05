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
#include <sys/mman.h>
#include <sys/types.h>

#include "kronos/configure_write_files.h"
#include "kronos/file_write.h"
#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/stats.h"
#include "kronos/trace.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/**
 * Return a handle to a static logger instance, ensuring that it is correctly
 * registered
 */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_bytes_logger("write");

    return logger;
}

FileWriteParamsInternal get_write_params(const FileWriteConfig* config) {

    FileWriteParamsInternal params;

    /* The writes are shared out as uniformly as possible across the processors */

    assert(config->writes > 0);

    params.num_writes = global_distribute_work(config->writes);

    /* Unlike for the reads, we can write arbitrary sizes, so we can just divide the amount to write
     * by the number of writes */

    /* n.b. Convert kb to bytes */
    params.write_size = 1024 * config->kilobytes / config->writes;

    return params;
}


static long process_write_counter() {

    static size_t counter = 0;
    return counter++;
}


int get_file_write_name(char* path_out, size_t max_len) {

    const GlobalConfig* global_conf = global_config_instance();
    int written;

    written = snprintf(path_out, max_len, "%s/%s-%li-%li", global_conf->file_write_path,
                       global_conf->hostname, (long)getpid(), process_write_counter());

    return (written > 0 && written < PATH_MAX) ? 0 : -1;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and write
 *        the data from the buffer using the mmap procedure
 */
static bool file_write_mmap(const char* file_path, long write_size) {

    int fd;
    char* pmapped;
    char* buffer;
    bool success;
    long chunk_size, offset, remaining;

    TRACE();

    success = false;

    fd = open(file_path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd != -1) {

        if (ftruncate(fd, write_size) == 0) {

            /*pmapped = mmap(NULL, params.write_size, PROT_WRITE, MAP_SHARED | MAP_FILE, fd, 0);*/
            pmapped = mmap(NULL, write_size, PROT_WRITE, MAP_SHARED, fd, 0);

            if (pmapped != MAP_FAILED) {

                /* Loop over chunks of the maximum chunk size, until all written */

                remaining = write_size;
                offset = 0;
                while (remaining > 0) {

                    chunk_size = remaining < file_write_max_chunk_size ? remaining : file_write_max_chunk_size;
                    assert(chunk_size > 0);
                    TRACE2("Writing chunk of: %li bytes", chunk_size);

                    buffer = malloc(chunk_size);

                    memcpy(pmapped + offset, buffer, chunk_size);

                    free(buffer);
                    buffer = NULL;

                    remaining -= chunk_size;
                    offset += chunk_size;
                }

                success = true;
                munmap(pmapped, write_size);

            } else {
                fprintf(stderr, "mmap error: (%d) %s\n", errno, strerror(errno));
            }
            close(fd);
        } else {
            fprintf(stderr, "Error setting file size %s (%d): %s\n", file_path, errno, strerror(errno));
        }
    } else {
        fprintf(stderr, "Failed to create file %s (%d): %s\n", file_path, errno, strerror(errno));
    }

    return success;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and write
 *        the data from the buffer using the standard C api
 */
static bool file_write_c_api(const char* file_path, long write_size) {

    FILE* file;
    char* buffer;
    bool success;
    long chunk_size, remaining, result;

    success = true;

    file = fopen(file_path, "wb");
    if (file != NULL) {

        /* Loop over chunks of the maximum chunk size, until all written */

        remaining = write_size;
        while (remaining > 0) {

            chunk_size = remaining < file_write_max_chunk_size ? remaining : file_write_max_chunk_size;
            assert(chunk_size > 0);
            TRACE2("Writing chunk of: %li bytes", chunk_size);

            buffer = malloc(chunk_size);

            stats_start(stats_instance());

            result = fwrite(buffer, 1, chunk_size, file);

            TRACE1("Flushing chunk.");
            fflush(file);

            stats_stop_log_bytes(stats_instance(), chunk_size);

            free(buffer);
            buffer = NULL;

            if (result != chunk_size) {
                success = false;
                fprintf(stderr, "A write error occurred reading file (%d) %s\n", errno, strerror(errno));
            }

            remaining -= chunk_size;
        }

        fclose(file);
    } else {
        success = false;
        fprintf(stderr, "Failed to create file %s (%d): %s\n", file_path, errno, strerror(errno));
    }

    return success;

}


static int execute_file_write(const void* data) {

    const FileWriteConfig* config = data;

    FileWriteParamsInternal params = get_write_params(config);

    int error;
    long count;
    bool success;
    char file_path[PATH_MAX];

    TRACE3("Writing %li files, %li bytes each", params.num_writes, params.write_size);

    error = 0;
    for (count = 0; count < params.num_writes; count++) {

        success = false;

        if (get_file_write_name(file_path, PATH_MAX) == 0) {

            TRACE2("Writing file %s ...", file_path);

            if (config->mmap) {
                success = file_write_mmap(file_path, params.write_size);
            } else {
                success = file_write_c_api(file_path, params.write_size);
            }

            TRACE1("... done");
        }

        /* And some error handling */

        if (!success) {
            fprintf(stderr, "A write error occurred on file %s\n", file_path);
            error = -1;
        }
    }

    return error;
}


KernelFunctor* init_file_write(const JSON* config_json) {

    FileWriteConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FileWriteConfig));

    if (json_object_get_integer(config_json, "kb_write", &config->kilobytes) != 0 ||
        json_object_get_integer(config_json, "n_write", &config->writes) != 0 ||
        config->kilobytes < 0 ||
        config->writes <= 0) {

        fprintf(stderr, "Invalid parameters specified in file-write config\n");
        free(config);
        return NULL;
    }

    if (json_object_get_boolean(config_json, "mmap", &config->mmap) != 0) {
        config->mmap = false;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_file_write;
    functor->data = config;

    return functor;
}
