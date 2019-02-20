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

#include "common/bool.h"
#include "kronos/global_config.h"
#include "kronos/kronos_version.h"
#include "kronos/mpi_kernel.h"
#include "kronos/stats.h"
#include "kronos/trace.h"
#include "common/utility.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <math.h>

static void write_kresults(const char* filename);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Track the duration of each frame */

typedef struct TimeSeriesFrame {

    double duration;
    struct TimeSeriesFrame* next;

} TimeSeriesFrame;

/* Data chunks may be associated with a particular frame, and carry the data (sparse) */

typedef struct TimeSeriesChunk {

    double value;
    unsigned long int chunkNumber;
    struct TimeSeriesChunk* next;

} TimeSeriesChunk;


typedef struct StatisticsRegistry {

    StatisticsLogger* loggers;
    TimeSeriesLogger* timeSeriesLoggers;

    /* Time series data */

    TimeSeriesFrame * first;
    TimeSeriesFrame * last;
    unsigned long int frameCount;

    double frameStartTime;
    bool timing;

} StatisticsRegistry;



static StatisticsRegistry* stats_instance() {

    static StatisticsRegistry registry;
    static bool initialised = false;

    if (!initialised) {
        registry.loggers = 0;
        registry.timeSeriesLoggers = 0;
        registry.first = 0;
        registry.last = 0;
        registry.frameCount = 0;
        registry.timing = false;
        initialised = true;
    }

    return &registry;
}


static void free_stats_logger(StatisticsLogger *logger) {

    assert(logger->name != 0);
    free(logger->name);

    free(logger);
}

static void free_ts_logger(TimeSeriesLogger *logger) {

    TimeSeriesChunk* chunk = logger->chunks;
    TimeSeriesChunk* next;

    assert(logger->name != 0);
    free(logger->name);

    chunk = logger->chunks;
    while (chunk != 0) {
        next = chunk->next;
        free(chunk);
        chunk = next;
    }

    free(logger);
}

/**
 * Clear up the statistics registry.
 * @note The entries point at StatisticsLogger objects that are owned inside the
 *       kernels. They should not be deleted from here.
 */
void free_stats_registry() {

    StatisticsRegistry* registry = stats_instance();

    StatisticsLogger* logger;
    StatisticsLogger* nextLogger;
    TimeSeriesLogger* tsLogger;
    TimeSeriesLogger* nextTsLogger;

    TimeSeriesFrame* frame;
    TimeSeriesFrame* nextFrame;

    /* Clean the main statistics loggers */

    logger = registry->loggers;
    while (logger != 0) {
        nextLogger = logger->next;
        free_stats_logger(logger);
        logger = nextLogger;
    }

    registry->loggers = 0;

    /* Clean up the logged frames */

    frame = registry->first;
    while (frame != 0) {
        nextFrame = frame->next;
        free(frame);
        frame = nextFrame;
    }

    registry->first = 0;
    registry->last = 0;
    registry->frameCount = 0;

    /* Clean up the time series loggers */

    tsLogger = registry->timeSeriesLoggers;
    while (tsLogger != 0) {
        nextTsLogger = tsLogger->next;
        free_ts_logger(tsLogger);
        tsLogger = nextTsLogger;
    }

    registry->timeSeriesLoggers = 0;
}


static StatisticsLogger* create_stats_logger(const char* name) {

    StatisticsRegistry* registry = stats_instance();

    int test_val; /* Avoid warnings with GNU + pedantic */

    /* Ensure that an existing logger does not exist with this name */
    StatisticsLogger* logger = registry->loggers;
    while (logger != 0) {
        test_val = logger->name == 0 ? 0 : strcmp(name, logger->name);
        assert(test_val != 0);
        logger = logger->next;
    }

    logger = malloc(sizeof(StatisticsLogger));
    logger->name = strdup(name);

    logger->count = 0;
    logger->sumBytes = 0;
    logger->sumBytesSquared = 0;
    logger->sumTimes = 0;
    logger->sumTimesSquared = 0;

    logger->logTimes = false;
    logger->logBytes = false;

    /* Insert into the linked list */
    logger->next = registry->loggers;
    registry->loggers = logger;

    return logger;
}


void stats_start(StatisticsLogger* logger) {

    assert(logger->logTimes);
    logger->startTime = take_time();
}


void stats_log_event(StatisticsLogger* logger) {

    assert(!logger->logBytes);
    assert(!logger->logTimes);

    logger->count++;
}


void stats_stop_log(StatisticsLogger* logger, unsigned long repetitions) {

    double elapsed;

    assert(!logger->logBytes);
    assert(logger->logTimes);

    elapsed = take_time() - logger->startTime;
    assert(elapsed >= 0);

    logger->count += repetitions;

    logger->sumTimes += elapsed;
    logger->sumTimesSquared += elapsed * elapsed;
}


void stats_stop_log_bytes(StatisticsLogger* logger, unsigned long bytes) {

    double elapsed;

    assert(logger->logBytes);
    assert(logger->logTimes);

    elapsed = take_time() - logger->startTime;
    assert(elapsed >= 0);

    logger->count++;

    logger->sumTimes += elapsed;
    logger->sumTimesSquared += elapsed * elapsed;

    logger->sumBytes += bytes;
    logger->sumBytesSquared += bytes * bytes;
}


StatisticsLogger *create_stats_times_logger(const char* name) {

    StatisticsLogger* logger = create_stats_logger(name);

    logger->logTimes = true;

    return logger;
}

StatisticsLogger *create_stats_times_bytes_logger(const char* name) {

    StatisticsLogger* logger = create_stats_logger(name);

    logger->logTimes = true;
    logger->logBytes = true;

    return logger;
}

void start_time_series_logging() {

    StatisticsRegistry* registry = stats_instance();
    assert(!registry->timing);
    assert(registry->first == 0);
    assert(registry->last == 0);

    registry->frameStartTime = take_time();
    registry->timing = true;
}

TimeSeriesLogger *register_time_series(const char *name) {

    StatisticsRegistry* registry = stats_instance();

    int test_val; /* Avoid warnings with GNU + pedantic */

    /* Ensure that an existing time series logger does not exist with this name */

    TimeSeriesLogger* logger = registry->timeSeriesLoggers;
    while (logger != 0) {
        test_val = logger->name == 0 ? 0 : strcmp(name, logger->name);
        assert(test_val != 0);
        logger = logger->next;
    }

    logger = malloc(sizeof(TimeSeriesLogger));
    logger->name = strdup(name);

    logger->chunks = 0;
    logger->count = 0;

    /* Insert into the linked list */

    logger->next = registry->timeSeriesLoggers;
    registry->timeSeriesLoggers = logger;

    return logger;
}

void log_time_series_chunk() {

    StatisticsRegistry* registry = stats_instance();

    double endTime = take_time();
    double elapsed = endTime - registry->frameStartTime;

    TimeSeriesFrame* frame = malloc(sizeof(TimeSeriesFrame));

    frame->duration = elapsed;

    /* Add this frame as the last in a series */

    frame->next = 0;

    if (registry->first == 0) {
        registry->first = frame;
    } else {
        registry->last->next = frame;
    }

    registry->last = frame;

    /* We start timing the next frame synchronously with the end of this one, to
     * ensure the generated time series is contiguous */
    registry->frameStartTime = endTime;
    registry->frameCount++;
}

void log_time_series_add_chunk_data(TimeSeriesLogger* logger, double value) {

    StatisticsRegistry* registry = stats_instance();
    TimeSeriesChunk* chunk;

    assert(logger != 0);

    chunk = malloc(sizeof(TimeSeriesChunk));

    chunk->chunkNumber = registry->frameCount;
    chunk->value = value;

    chunk->next = logger->chunks;
    logger->chunks = chunk;

    logger->count++;
}

static double calc_stddev(unsigned long int count, double sum, double sumSquares) {

    double intermediate;

    if (count == 0) return 0;

    /* Ensure we catch the case where the std deviation should be zero, but we end
     * up taking the square root of a very small negative number due to floating
     * point rounding errors */
    intermediate = (count * sumSquares) - (sum * sum);
    if (intermediate < 0) return 0;

    return sqrt(intermediate) / count;
}


static void report_logger(FILE* fp, const StatisticsLogger* logger) {

    const GlobalConfig* global_conf = global_config_instance();

    double average;
    double stddev;

    /* Report counts */

    fprintf(fp, "%s:%d %s count: %lu\n",
           global_conf->hostname,
           global_conf->pid,
           logger->name,
           logger->count);

    /* Report data stats */

    if (logger->logBytes) {

        average = logger->count ?
                    (double)logger->sumBytes / (double)logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumBytes, logger->sumBytesSquared);

        fprintf(fp, "%s:%d %s bytes (tot, avg, std): %lu, %lu, %lu\n",
               global_conf->hostname,
               global_conf->pid,
               logger->name,
               logger->sumBytes,
               (unsigned long)average,
               (unsigned long)stddev);
    }

    /* Report timing stats */

    if (logger->logTimes) {

        average = logger->count ? logger->sumTimes / logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumTimes, logger->sumTimesSquared);

        fprintf(fp, "%s:%d %s times (tot, avg, std): %es, %es, %es\n",
               global_conf->hostname,
               global_conf->pid,
               logger->name,
               logger->sumTimes,
               average,
               stddev);
    }
}


void report_stats() {

    const GlobalConfig* global_conf = global_config_instance();

    StatisticsRegistry* registry = stats_instance();

    /* Report statistics to stdout */

    if (global_conf->print_statistics) {
        const StatisticsLogger* logger = registry->loggers;
        while (logger != 0) {
            report_logger(stdout, logger);
            logger = logger->next;
        }
    }

    /* Build and output JSONs */

    if (global_conf->write_statistics_file)
        write_kresults(global_conf->statistics_file);

}


static JSON* logger_json(const StatisticsLogger* logger) {

    JSON* json;
    double average;
    double stddev;

    /* The logging details */

    json = json_object_new();
    json_object_insert(json, "count", json_number_new(logger->count));

    if (logger->logBytes) {
        average = logger->count ?
                    (double)logger->sumBytes / (double)logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumBytes, logger->sumBytesSquared);

        json_object_insert(json, "bytes", json_number_new(logger->sumBytes));
        json_object_insert(json, "averageBytes", json_number_new(average));
        json_object_insert(json, "sumSquaredBytes", json_number_new(logger->sumBytesSquared));
        json_object_insert(json, "stddevBytes", json_number_new(stddev));
    }

    if (logger->logTimes) {
        average = logger->count ? logger->sumTimes / logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumTimes, logger->sumTimesSquared);

        json_object_insert(json, "elapsed", json_number_new(logger->sumTimes));
        json_object_insert(json, "averageElapsed", json_number_new(average));
        json_object_insert(json, "sumSquaredElapsed", json_number_new(logger->sumTimesSquared));
        json_object_insert(json, "stddevElapsed", json_number_new(stddev));
    }

    return json;
}


JSON* time_series_json() {

    StatisticsRegistry* registry = stats_instance();

    JSON* ts_json = json_object_new();

    int count = registry->frameCount;

    int i;
    double* arr;

    TimeSeriesFrame* frame;
    TimeSeriesChunk* chunk;
    TimeSeriesLogger* logger;

    /* Build sequence of times */

    arr = malloc(sizeof(double) * count);
    for (i = 0; i < count; i++) {
        arr[i] = 0;
    }

    i = 0;
    frame = registry->first;
    while (frame != 0) {
        arr[i] = frame->duration;
        frame = frame->next;
        i++;
    }

    assert(i == count);

    json_object_insert(ts_json, "durations", json_array_from_array(arr, count));

    /* Add an entry for each of the logged time series. */

    logger = registry->timeSeriesLoggers;
    while (logger != 0) {

        for (i = 0; i < count; i++) {
            arr[i] = 0;
        }

        i = 0;
        chunk = logger->chunks;
        while (chunk != 0) {
            arr[chunk->chunkNumber] += chunk->value;
            chunk = chunk->next;
            i++;
        }

        json_object_insert(ts_json, logger->name, json_array_from_array(arr, count));

        logger = logger->next;
    }

    free(arr);

    return ts_json;
}


JSON* report_stats_json() {

    const GlobalConfig* global_conf = global_config_instance();

    StatisticsRegistry* registry = stats_instance();

    const StatisticsLogger* logger;

    JSON* json_stats = json_object_new();
    JSON* json;

    /* Report the aggregaed statistics */

    logger = registry->loggers;
    while (logger != 0) {
        json_object_insert(json_stats, logger->name, logger_json(logger));
        logger = logger->next;
    }

    /* Identify which rank we are, and construct the wrapper */

    json = json_object_new();
    json_object_insert(json, "stats", json_stats);
    json_object_insert(json, "time_series", time_series_json());
    json_object_insert(json, "host", json_string_new(global_conf->hostname));
    json_object_insert(json, "pid", json_number_new(global_conf->pid));
    json_object_insert(json, "rank", json_number_new(global_conf->mpi_rank));

    return json;
}

#define SEND_BUFFER_SIZE (1024 * 100)


static void write_kresults(const char* filename) {

    const GlobalConfig* global_conf = global_config_instance();

    FILE* fp;
    JSON* stats_json;
    JSON* json;
    JSON* aggregated_stats;
    time_t t;
    struct tm * tm;

    const int buff_size = SEND_BUFFER_SIZE;

    int send_size;
    int i, err, total_count, err_len;

    int* recvcounts = 0;
    int* offsets = 0;

    char send_buffer[SEND_BUFFER_SIZE];
#ifdef HAVE_MPI
    char error_string[MPI_MAX_ERROR_STRING];
#endif
    char date_buffer[50];
    char* recv_buffer = 0;
    bool success = true;

    /* TODO: Deal with the case where the buffer is too small! */
    /* TODO: Check for MPI errors, and handle them gracefully */

    /* Gather all of the statistics locally */

    stats_json = report_stats_json();

    if (global_conf->mpi_rank == 0) {

        aggregated_stats = json_array_new();
        json_array_append(aggregated_stats, stats_json);

#ifdef HAVE_MPI

        /* Recieve serialised JSONs from all of the other ranks (n.b. root does not send) */

        recvcounts = malloc(global_conf->nprocs * sizeof(int));
        offsets = malloc(global_conf->nprocs * sizeof(int));

        send_size = 0;
        err = MPI_Gather(&send_size, 1, MPI_INT, recvcounts, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (err != MPI_SUCCESS) {
            MPI_Error_string(err, error_string, &err_len);
            fprintf(stderr, "An error occurred in MPI_Gather: %s (%d)\n", send_buffer, err);
            success = false;
        }

        /* Determine the offsets for each received serialised JSON, and collect them on the root */

        if (success) {

            total_count = 0;
            for (i = 0; i < global_conf->nprocs; i++) {
                offsets[i] = total_count;
                total_count += recvcounts[i];
            }

            recv_buffer = malloc(total_count);

            err = MPI_Gatherv(send_buffer, send_size, MPI_CHAR, recv_buffer, recvcounts, offsets, MPI_CHAR, 0, MPI_COMM_WORLD);
            if (err != MPI_SUCCESS) {
                MPI_Error_string(err, error_string, &err_len);
                fprintf(stderr, "An error occurred in MPI_Gatherv: %s (%d)\n", send_buffer, err);
                success = false;
            }
        }

        /* Convert the strings back to JSONS and append them to the list. n.b. Loop from 1, as we
         * have already included the root node */

        if (success) {
            for (i = 1; i < global_conf->nprocs; i++) {
                json = json_from_string(&recv_buffer[offsets[i]]);
                if (json == 0) {
                    fprintf(stderr, "An error occurred getting statistics for rank: %d\n", i);
                    continue;
                }
                json_array_append(aggregated_stats, json);
            }
        }

        free(recv_buffer);
        free(offsets);
        free(recvcounts);
#endif

        /* Construct an outer KResults object */

        json = json_object_new();

        json_object_insert(json, "uid", json_number_new(global_conf->uid));
        json_object_insert(json, "tag", json_string_new("KRONOS-KRESULTS-MAGIC"));
        json_object_insert(json, "version", json_number_new(1));
        json_object_insert(json, "kronosVersion", json_string_new(kronos_version_str()));
        json_object_insert(json, "kronosSHA1", json_string_new(kronos_git_sha1()));
        json_object_insert(json, "ranks", aggregated_stats);

        /* Output the time according to rfc3339 */
        t = time(NULL);
        tm = localtime(&t);
        strftime(date_buffer, sizeof(date_buffer), "%Y-%m-%dT%H:%M:%S+00:00", tm);
        json_object_insert(json, "created", json_string_new(date_buffer));

        /* Write the KResults to disk (only the head node) */

        fp = fopen(global_conf->statistics_file, "w");
        if (fp != 0) {
            TRACE2("Writing statistics file to: %s", filename);
            write_json(fp, json);
            fclose(fp);
        } else {
            fprintf(stderr, "Failed to create statistics file: %s (%s)\n", filename, strerror(errno));
        }

        free_json(json);

    } else {

        /* On all other nodes, we just send the data to the head node */

        /* Serialise the JSON data into a buffer */
        send_size = write_json_string(send_buffer, buff_size, stats_json);
        if (send_size > buff_size) {
            fprintf(stderr, "Insufficient buffer size to transfer statistics json. Needs %d bytes\n", send_size);
            send_size = 0;
        }

#ifdef HAVE_MPI
        /* Send the sizes, and then the data */
        /* NOTE: recvtype specified to avoid segfault in MPICH. Should be ignored according to MPI standard */
        err = MPI_Gather(&send_size, 1, MPI_INT, 0, 0, MPI_INT, 0, MPI_COMM_WORLD);
        if (err != MPI_SUCCESS) {
            MPI_Error_string(err, error_string, &err_len);
            fprintf(stderr, "An error occurred in MPI_Gather: %s (%d)\n", send_buffer, err);
        } else {
            err = MPI_Gatherv(send_buffer, send_size, MPI_CHAR, 0, 0, 0, MPI_CHAR, 0, MPI_COMM_WORLD);
            if (err != MPI_SUCCESS) {
                MPI_Error_string(err, error_string, &err_len);
                fprintf(stderr, "An error occurred in MPI_Gatherv: %s (%d)\n", send_buffer, err);
            }
        }
#else
        fprintf(stderr, "Non-root node without MPI compiled in. App misconfigured\n");
#endif
        free_json(stats_json);
    }


}

/* ------------------------------------------------------------------------------------------------------------------ */
