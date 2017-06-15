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

#include "kronos/file_read.h"
#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/stats.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/**
 * Return a pointer to a registered logger. Ensure that it is correctly
 * initialised on the first call.
 */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_bytes_logger("read");

    return logger;
}

static TimeSeriesLogger* reads_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("n_read");

    return logger;
}

static TimeSeriesLogger* bytes_read_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("bytes_read");

    return logger;
}

FileReadParamsInternal get_read_params(const FileReadConfig* config) {

    const GlobalConfig* global_conf = global_config_instance();

    long total_size;
    FileReadParamsInternal params;

    assert(config->reads > 0);

    /* n.b. Convert kb to bytes */
    params.num_reads = global_distribute_work(config->reads);
    params.read_size = 1024 * config->kilobytes / config->reads;

    if (params.read_size < global_conf->file_read_size_min) {
        params.read_size = global_conf->file_read_size_min;
    }

    if (params.read_size > global_conf->file_read_size_max) {
        total_size = params.num_reads * params.read_size;

        params.num_reads = 1 + ((total_size - 1) / global_conf->file_read_size_max);
        params.read_size = global_conf->file_read_size_max;
    }

    return params;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and read
 *        the data into a buffer using the mmap procedure
 */
static bool file_read_mmap(const char* file_path, long offset, long read_size, char* buffer, bool invalidate) {

    int fd;
    char* pmapped;
    bool success;
    int err;

    const GlobalConfig* global_conf = global_config_instance();

    TRACE();

    success = false;

    stats_start(stats_instance());

    fd = open(file_path, O_RDONLY);
    if (fd != -1) {

        /* Map the whole file. Avoids working out page-aligned offsets. Reading is lazy anyway */
        pmapped = mmap(NULL, global_conf->file_read_size_max, PROT_READ, MAP_PRIVATE, fd, 0);
        /* pmapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE | MAP_FILE, fd, 0); */

        if (pmapped != MAP_FAILED) {
            success = true;
            memcpy(buffer, pmapped + offset, read_size);

            if (invalidate) {
#if defined(__linux__) || defined(__hpux)
                if ((err = posix_fadvise(fd, offset, read_size, POSIX_FADV_DONTNEED)) != 0)
                    fprintf(stderr, "Cache invalidation with posix_fadvise failed: %s\n", strerror(err));
#elif defined(__FreeBSD__) || defined(__sun__) || defined(__APPLE__)
                if (msync((void*)((unsigned long)pmapped+offset), read_size, MS_INVALIDATE) != 0)
                    fprintf(stderr, "Cache invalidation with msync failed: %s\n", strerror(errno));
#else
                fprintf(stderr, "File page mapping invalidation not supported on this platform");
#endif
            }

            munmap(pmapped, global_conf->file_read_size_max);
        } else {
            fprintf(stderr, "mmap error: (%d) %s\n", errno, strerror(errno));
        }
        close(fd);
    }

    stats_stop_log_bytes(stats_instance(), read_size);

    return success;
}


/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and read
 *        the data into a buffer using the standard C api
 * @return
 */
static bool file_read_c_api(const char* file_path, long offset, long read_size, char* buffer) {

    FILE* file;
    bool success;
    long result;
    int ret;

    TRACE();

    success = false;

    stats_start(stats_instance());

    /* TODO: If we want the option to use O_DIRECT, then we need to use POSIX open, not fopen */

    file = fopen(file_path, "rb");
    if (file != NULL) {

        ret = fseek(file, offset, SEEK_SET);

        if (ret == -1) {
            fprintf(stderr, "A error occurred seeking in file: %s (%d) %s\n", file_path, errno, strerror(errno));
        } else {

            result = fread(buffer, 1, read_size, file);

            if (result != read_size) {
                fprintf(stderr, "A read error occurred reading file: %s (%d) %s\n", file_path, errno, strerror(errno));
            } else {
                success = true;
            }
        }
        fclose(file);
    }

    stats_stop_log_bytes(stats_instance(), read_size);

    return success;

}


int execute_file_read(const void* data) {

    const FileReadConfig* config = data;
    const GlobalConfig* global_conf = global_config_instance();

    int count, file_index, written, error, offset;
    bool success;
    char file_path[PATH_MAX];
    char * read_buffer;

    FileReadParamsInternal params = get_read_params(config);

    TRACE3("Read %li files, %li bytes each.", params.num_reads, params.read_size);

    /* And do the actual reading!!! */

    read_buffer = malloc(params.read_size);

    error = 0;
    for (count = 0; count < params.num_reads; count++) {

        /* Pick a random file to read from */
        file_index = rand() % global_conf->file_read_multiplicity;

        /* Pick a random offset to read from within the file */
        offset = rand() % (global_conf->file_read_size_max - params.read_size);

        success = false;
        written = snprintf(file_path, PATH_MAX, "%s/read-cache-%d", global_conf->file_read_path, file_index);
        if (written > 0 && written < PATH_MAX) {

            TRACE4("Reading %ld bytes from file %s, offset %ld ...", params.read_size, file_path, offset);

            if (config->mmap) {
                success = file_read_mmap(file_path, offset, params.read_size, read_buffer, config->invalidate);
            } else {
                assert(!config->invalidate);
                success = file_read_c_api(file_path, offset, params.read_size, read_buffer);
            }

            TRACE1("... done");
        }

        /* And some error handling */

        if (!success) {
            fprintf(stderr, "A read error occurred on file %s\n", file_path);
            error = -1;
        }

        log_time_series_add_chunk_data(bytes_read_time_series(), params.read_size);
    }

    log_time_series_add_chunk_data(reads_time_series(), params.num_reads);
    log_time_series_chunk();

    free(read_buffer);

    return error;
}


KernelFunctor* init_file_read(const JSON* config_json) {

    FileReadConfig* config;
    KernelFunctor* functor;
    bool success;

    TRACE();

    config = malloc(sizeof(FileReadConfig));

    success = true;

    if (json_object_get_integer(config_json, "kb_read", &config->kilobytes) != 0 ||
        json_object_get_integer(config_json, "n_read", &config->reads) != 0 ||
        config->kilobytes < 0 ||
        config->reads <= 0) {

        fprintf(stderr, "Invalid parameters specified in file-read config\n");
        success = false;
    }

    if (json_object_get_boolean(config_json, "mmap", &config->mmap) != 0) {
        config->mmap = false;
    }

    /*if (json_object_get_boolean(config_json, "o_direct", &config->o_direct) != 0) {*/
        config->o_direct = false;
    /*}*/

    if (json_object_get_boolean(config_json, "invalidate", &config->invalidate) != 0) {
        config->invalidate = false;
    }

    if (config->invalidate && !config->mmap) {
        fprintf(stderr, "Read cache invalidation only supported using mmap. Please set mmap=true");
        success = false;
    }

    /*if (config->o_direct && config->mmap) {
        fprintf(stderr, "O_DIRECT is not meaningful with mmap.");
        success = false;
    }*/

    if (success) {
        functor = malloc(sizeof(KernelFunctor));
        functor->next = NULL;
        functor->execute = &execute_file_read;
        functor->data = config;
    } else {
        free(config);
        functor = NULL;
    }

    return functor;
}
