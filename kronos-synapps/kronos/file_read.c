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
#include <sys/stat.h>
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

    long total_size, max_read_size;
    int i;
    struct stat st;
    FileReadParamsInternal params;

    assert(config->reads > 0);

    /* n.b. Convert kb to bytes */
    params.num_reads = global_distribute_work(config->reads);
    params.read_size = 1024 * config->kilobytes / config->reads;
    params.file_sizes = 0;

    if (params.read_size < global_conf->file_read_size_min) {
        params.read_size = global_conf->file_read_size_min;
    }

    /* Check that all referenced files exist, and have appropriate sizes */

    max_read_size = global_conf->file_read_size_max;

    if (config->file_list) {

        assert(config->nfiles > 0);
        params.file_sizes = calloc(config->nfiles, sizeof(long));

        for (i = 0; i < config->nfiles; i++) {
            assert(config->file_list[i] != 0);
            if (stat(config->file_list[i], &st) == -1) {
                fprintf(stderr, "Could not get file size for %s in file-read (%d): %s\n", config->file_list[i], errno, strerror(errno));
                free(params.file_sizes);
                params.file_sizes = 0; /* Reports the error */
                break;
            }
            params.file_sizes[i] = st.st_size;
            if (st.st_size < max_read_size) max_read_size = st.st_size;
        }
    }

    /* Ensure that all reads are small enough for the source files */

    if (params.read_size > max_read_size) {
        total_size = params.num_reads * params.read_size;

        params.num_reads = 1 + ((total_size - 1) / max_read_size);
        params.read_size = max_read_size;
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

/*
 * Select a file to read from at random.
 */

static long select_random_read_path(const FileReadConfig* config, const FileReadParamsInternal* params, char* file_path, long path_len, int* offset) {

    const GlobalConfig* global_conf = global_config_instance();

    int file_index;
    long file_size;

    /* Select a random file from those available, either in supplied list, or globally */

    if (config->file_list) {

        assert(config->nfiles > 0);
        file_index = rand() % config->nfiles;
        strncpy(file_path, config->file_list[file_index], path_len);
        path_len = strlen(config->file_list[file_index]);

        file_size = params->file_sizes[file_index];

    } else {

        /* Use the global read cache */

        file_index = rand() % global_conf->file_read_multiplicity;
        path_len = snprintf(file_path, PATH_MAX, "%s/read-cache-%d", global_conf->file_read_path, file_index);

        file_size = global_conf->file_read_size_max;
    }

    /* Where in the file should we read from? */

    assert(params->read_size <= file_size);
    if (params->read_size == file_size) {
        *offset = 0;
    } else {
        *offset = rand() % (file_size - params->read_size);
    }

    *offset = 0;
    return path_len;
}


int execute_file_read(const void* data) {

    const FileReadConfig* config = data;

    int count, written, error, offset;
    bool success;
    char file_path[PATH_MAX];
    char * read_buffer;

    FileReadParamsInternal params = get_read_params(config);

    if (config->file_list != 0 && params.file_sizes == 0) {
        fprintf(stderr, "Error getting file information for specified file list in file-read\n");
        return -1;
    }

    TRACE3("Read %li files, %li bytes each.", params.num_reads, params.read_size);

    /* And do the actual reading!!! */

    read_buffer = malloc(params.read_size);

    error = 0;
    for (count = 0; count < params.num_reads; count++) {

        written = select_random_read_path(config, &params, file_path, sizeof(file_path), &offset);
        success = false;

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

    if (params.file_sizes != 0) free(params.file_sizes);
    free(read_buffer);

    return error;
}


static void free_data_file_read(void* data) {

    FileReadConfig* config = data;
    int i;

    /* Clean up the list of files */
    if (config->file_list) {
        assert(config->nfiles > 0);
        for (i = 0; i < config->nfiles; i++) {
            if (config->file_list[i]) free(config->file_list[i]);
        }
        free(config->file_list);
    }

    free(config);
}


KernelFunctor* init_file_read(const JSON* config_json) {

    const GlobalConfig* global_conf = global_config_instance();

    FileReadConfig* config;
    KernelFunctor* functor;
    const char* relative_path;
    const JSON* files_list;
    bool success;
    int i;

    TRACE();

    config = malloc(sizeof(FileReadConfig));
    config->file_list = 0;
    config->nfiles = 0;

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

    /* Are we specifying explicitly the files to read from? */

    files_list = json_object_get(config_json, "files");
    if (files_list) {

        if (!json_is_array(files_list)) {
            fprintf(stderr, "Invalid files list specified for file-read kernel\n");
            success = false;
        }

        if (success) {
            config->nfiles = json_array_length(files_list);
            if (config->nfiles < 1) {
                fprintf(stderr, "At least one file is required for specifying read source in file-read\n");
                success = false;
            }
        }

        if (success) {

            config->file_list = calloc(config->nfiles, sizeof(char*));

            for (i = 0; i < config->nfiles; i++) {

                if (json_as_string_ptr(json_array_element(files_list, i), &relative_path) != 0) {
                    fprintf(stderr, "Invalid path specified in files field for file-read\n");
                    success = false;
                    break;
                };

                config->file_list[i] = malloc(PATH_MAX);
                strcpy(config->file_list[i], global_conf->file_shared_path);
                strcat(config->file_list[i], "/");
                strcat(config->file_list[i], relative_path);
            }
        }
    }

    if (success) {
        functor = malloc(sizeof(KernelFunctor));
        functor->next = NULL;
        functor->execute = &execute_file_read;
        functor->free_data = &free_data_file_read;
        functor->data = config;
    } else {
        free_data_file_read(config);
        functor = NULL;
    }

    return functor;
}
