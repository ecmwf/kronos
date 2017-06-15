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
 * Return a pointer to a registered logger. Ensure that it is correctly
 * initialised on the first call.
 */

static StatisticsLogger* stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_bytes_logger("write");

    return logger;
}

static TimeSeriesLogger* writes_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("n_write");

    return logger;
}

static TimeSeriesLogger* bytes_written_time_series() {

    static TimeSeriesLogger* logger = 0;

    if (logger == 0)
        logger = register_time_series("bytes_write");

    return logger;
}

/* Kernel configuration */


FileWriteParamsInternal get_write_params(const FileWriteConfig* config) {

    const GlobalConfig* global_conf = global_config_instance();
    FileWriteParamsInternal params;
    long nfiles;

    /* The writes are shared out as uniformly as possible across the processors */

    assert(config->writes > 0);

    params.num_writes = global_distribute_work(config->writes);

    nfiles = (global_conf->nprocs > config->files) ? global_conf->nprocs : config->files;
    params.num_files = global_distribute_work(nfiles);

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


/*
 * Track the currently open files
 */

typedef struct WriteFileInfo {

    char filename[PATH_MAX];
    int fd;

    struct WriteFileInfo* next;

} WriteFileInfo;


static WriteFileInfo* global_file_list = 0;
static int global_file_count = 0;


void close_write_files() {

    WriteFileInfo* file_info = global_file_list;
    WriteFileInfo* next;
    int count = 0;

    while(file_info) {

        count++;

        TRACE2("Closing file: %s", file_info->filename);
        close(file_info->fd);

        next = file_info->next;
        free(file_info);
        file_info = next;
    }

    if (count != global_file_count) {
        fprintf(stderr, "non-fatal ERROR: Incorrect number of files closed: %d, expected %d\n",
                count, global_file_count);
    }

    global_file_count = 0;
    global_file_list = 0;
}


bool open_write_file(bool o_direct) {

    int flags;

    WriteFileInfo* file_info = malloc(sizeof(WriteFileInfo));

    if (get_file_write_name(file_info->filename, sizeof(file_info->filename)) == 0) {

        flags = O_WRONLY | O_CREAT;
        assert(!o_direct);
        /*if (o_direct)
            flags |= O_DIRECT;*/

        TRACE2("Creating and opening file: %s", file_info->filename);
        file_info->fd = open(file_info->filename, flags);

        if (file_info->fd == -1) {
            fprintf(stderr, "An error occurred opening the file %s for write: %d (%s)\n",
                    file_info->filename, errno, strerror(errno));
            free(file_info);
            return false;
        }

        file_info->next = global_file_list;
        global_file_list = file_info;

        global_file_count++;

    } else {
        fprintf(stderr, "An error occurred getting the write filename\n");
        free(file_info);
        return false;
    }

    return true;
}

bool write_to_file(int fd, long size) {

    long remaining;
    long chunk_size;
    int result;
    bool success;
    char* buffer = 0;

    /* Loop over chunks of the maximum chunk size, until all written */

    success = true;
    remaining = size;
    while (remaining > 0) {

        chunk_size = remaining < file_write_max_chunk_size ? remaining : file_write_max_chunk_size;
        assert(chunk_size > 0);
        TRACE2("Writing chunk of: %li bytes ", chunk_size);

        buffer = malloc(chunk_size);

        stats_start(stats_instance());

        result = write(fd, buffer, chunk_size);

        stats_stop_log_bytes(stats_instance(), chunk_size);

        free(buffer);
        buffer = 0;

        if (result == -1) {
            fprintf(stderr, "A write error occurred: %d (%s)\n", errno, strerror(errno));
            success = false;
        }

        remaining -= chunk_size;
    }

    return success;
}


/* Disabled to implement new write behaivour. This should be fixed */
#if 0
/**
 * @brief file_read_mmap Given a specified filename, size and an appropriate buffer, open and write
 *        the data from the buffer using the mmap procedure
 */
static bool file_write_mmap(const char* file_path, long write_size) {

    int fd;
    char* pmapped;
    char* buffer;
    bool success;
    long chunk_size, offset, remaining, aligned, aligned_size, aligned_remainder, pagesize;

    TRACE();

    success = false;

    pagesize = sysconf(_SC_PAGESIZE);
    TRACE2("Page size: %li", pagesize);

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

                    stats_start(stats_instance());

                    memcpy(pmapped + offset, buffer, chunk_size);

                    TRACE1("Flushing chunk.");

                    aligned_size = chunk_size + ((unsigned long)(pmapped + offset) % pagesize);
                    aligned_remainder = aligned_size % pagesize;
                    if (aligned_remainder != 0)
                        aligned_size += pagesize - aligned_remainder;
                    aligned = ((unsigned long)pmapped + offset) - ((unsigned long)(pmapped + offset) % pagesize);
                    msync((void*)aligned, chunk_size, MS_SYNC);

                    stats_stop_log_bytes(stats_instance(), chunk_size);

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

    log_time_series_add_chunk_data(bytes_written_time_series(), params.num_writes * params.write_size);
    log_time_series_add_chunk_data(writes_time_series(), params.num_writes);
    log_time_series_chunk();

    return error;
}
#endif

static int execute_file_write(const void* data) {

    const FileWriteConfig* config = data;

    FileWriteParamsInternal params = get_write_params(config);
    const WriteFileInfo* file_info;

    int i, file_cnt, error;
    char* buffer;

    assert(!config->mmap);

    error = 0;

    TRACE4("Writing to %li files, %li writes of %li bytes each", params.num_files, params.num_writes, params.write_size);

    /* If we don't have enough open files, open more files! */

    while (global_file_count < params.num_files) {
        if (!open_write_file(config->o_direct)) {
            fprintf(stderr, "An error occurred in the file write kernel");
            error = -1;
            return;
        }
    }

    /* Do the writes! */

    file_info = global_file_list;
    for (file_cnt = 0; file_cnt < params.num_writes; file_cnt++) {

        TRACE3("Writing %li bytes to %s", params.write_size, file_info->filename);
        if (!write_to_file(file_info->fd, params.write_size)) {
            fprintf(stderr, "A write error occurred on file: %s\n", file_info->filename);
            error = -1;
        }

        /* Loop around the available files */
        if ((file_cnt + 1) % params.num_files == 0) {
            file_info = global_file_list;
        } else {
            file_info = file_info->next;
        }
    }

    /* Flush everything */

    file_info = global_file_list;
    for (file_cnt = 0; file_cnt < params.num_files; file_cnt++) {
        TRACE2("Syncing file: %s\n", file_info->filename);
        fsync(file_info->fd);
        file_info = file_info->next;
    }

    /* If configured to do so, close the relevant files */

    if (!config->continue_files)
        close_write_files();

    log_time_series_add_chunk_data(bytes_written_time_series(), params.num_writes * params.write_size);
    log_time_series_add_chunk_data(writes_time_series(), params.num_writes);
    log_time_series_chunk();

    return error;
}


KernelFunctor* init_file_write(const JSON* config_json) {

    FileWriteConfig* config;
    KernelFunctor* functor;

    TRACE();

    config = malloc(sizeof(FileWriteConfig));

    if (json_object_get_integer(config_json, "kb_write", &config->kilobytes) != 0 ||
        json_object_get_integer(config_json, "n_write", &config->writes) != 0 ||
        json_object_get_integer(config_json, "n_files", &config->files) != 0 ||
        config->kilobytes < 0 ||
        config->writes <= 0 ||
        config->files <= 0) {

        fprintf(stderr, "Invalid parameters specified in file-write config\n");
        free(config);
        return NULL;
    }

    if (json_object_get_boolean(config_json, "mmap", &config->mmap) != 0) {
        config->mmap = false;
    }

    /*if (json_object_get_boolean(config_json, "o_direct", &config->o_direct) != 0) {*/
        config->o_direct = false;
    /*}*/

    if (json_object_get_boolean(config_json, "continue_files", &config->continue_files) != 0) {
        config->continue_files = true;
    }

    if (config->mmap) {
        fprintf(stderr, "mmap currently not supported for file-tracking behaviour. See NEX-114");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_file_write;
    functor->data = config;

    return functor;
}
