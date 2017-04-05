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

#include "kronos/stats.h"
#include "kronos/bool.h"
#include "kronos/global_config.h"
#include "kronos/utility.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>

/* ------------------------------------------------------------------------------------------------------------------ */

typedef struct StatisticsRegistry {

    StatisticsLogger* loggers;

} StatisticsRegistry;


static StatisticsRegistry* stats_instance() {

    static StatisticsRegistry registry;
    static bool initialised = false;

    if (!initialised) {
        registry.loggers = 0;
        initialised = true;
    }

    return &registry;
}


static void free_stats_logger(StatisticsLogger *logger) {

    assert(logger->name != 0);
    free(logger->name);

    free(logger);
}

/**
 * Clear up the statistics registry.
 * @note The entries point at StatisticsLogger objects that are owned inside the
 *       kernels. They should not be deleted from here.
 */
void free_stats_registry() {

    StatisticsRegistry* registry = stats_instance();
    StatisticsLogger* logger = registry->loggers;

    while (logger != 0) {
        StatisticsLogger* next = logger->next;
        free_stats_logger(logger);
        logger = next;
    }

    registry->loggers = 0;
}


static StatisticsLogger* create_stats_logger(const char* name) {

    StatisticsRegistry* registry = stats_instance();

    /* Ensure that an existing logger does not exist with this name */
    StatisticsLogger* logger = registry->loggers;
    while (logger != 0) {
        assert(logger->name != 0 && strcmp(name, logger->name) != 0);
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


void stats_stop_log(StatisticsLogger* logger) {

    assert(!logger->logBytes);
    assert(logger->logTimes);

    double elapsed = take_time() - logger->startTime;
    assert(elapsed >= 0);

    logger->count++;

    logger->sumTimes += elapsed;
    logger->sumTimesSquared += elapsed * elapsed;
}


void stats_stop_log_bytes(StatisticsLogger* logger, unsigned long bytes) {

    assert(logger->logBytes);
    assert(logger->logTimes);

    double elapsed = take_time() - logger->startTime;
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

    fprintf(fp, "%s:%d %s count: %d\n",
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


void report_stats(FILE* fp) {

    StatisticsRegistry* registry = stats_instance();

    const StatisticsLogger* logger = registry->loggers;
    while (logger != 0) {
        report_logger(fp, logger);
        logger = logger->next;
    }
}


static JSON* logger_json(const StatisticsLogger* logger) {

    JSON* json = json_object_new();
    double average;
    double stddev;

    json_object_insert(json, "count", json_number_new(logger->count));

    if (logger->logBytes) {
        average = logger->count ?
                    (double)logger->sumBytes / (double)logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumBytes, logger->sumBytesSquared);

        json_object_insert(json, "bytes", json_number_new(logger->sumBytes));
        json_object_insert(json, "averageBytes", json_number_new(average));
        json_object_insert(json, "stddevBytes", json_number_new(stddev));
    }

    if (logger->logTimes) {
        average = logger->count ? logger->sumTimes / logger->count : 0;
        stddev = calc_stddev(logger->count, logger->sumTimes, logger->sumTimesSquared);

        json_object_insert(json, "elapsed", json_number_new(logger->sumTimes));
        json_object_insert(json, "averageElapsed", json_number_new(average));
        json_object_insert(json, "stddevElapsed", json_number_new(stddev));
    }

    return json;
}


JSON* report_stats_json() {

    /* TODO
     *
     * i) Build JSONS for each logger
     * ii) Aggregate into a JSON for a process
     * iii) If we are using MPI, bring each of these JSONS back to the head node
     * iv) Annotate with KPR macroscopic information
     * v) Create a wrapper routine that dumps to file.
     */

    StatisticsRegistry* registry = stats_instance();

    const StatisticsLogger* logger;

    JSON* json = json_object_new();

    logger = registry->loggers;
    while (logger != 0) {
        json_object_insert(json, logger->name, logger_json(logger));
        logger = logger->next;
    }

    return json;
}

/* ------------------------------------------------------------------------------------------------------------------ */
