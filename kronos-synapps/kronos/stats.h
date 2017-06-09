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
 * @date June 2016
 * @author Simon Smart
 */


#ifndef kronos_stats_H
#define kronos_stats_H

#include "kronos/bool.h"
#include "kronos/json.h"

#include <stdio.h>

/* ------------------------------------------------------------------------------------------------------------------ */

/* A statistics logger that can be attached to a kernel */

typedef struct StatisticsLogger {

    char* name;

    unsigned long int count;

    unsigned long int sumBytes;
    unsigned long int sumBytesSquared;

    double sumTimes;
    double sumTimesSquared;

    bool logTimes;
    bool logBytes;

    double startTime;

    struct StatisticsLogger* next;

} StatisticsLogger;

/* A time series logger that can be attached to a kernel */

struct TimeSeriesChunk;

typedef struct TimeSeriesLogger {

    char* name;

    unsigned long int count;

    struct TimeSeriesChunk* chunks;

    struct TimeSeriesLogger* next;

} TimeSeriesLogger;


/**
 * Obtain access to a statistics logger for the specified name.
 * @note The receiver does not get _ownership_ of the stats logger, and should
 *       not attempt to clean it up.
 */

StatisticsLogger* create_stats_times_logger(const char* name);
StatisticsLogger* create_stats_times_bytes_logger(const char* name);
void free_stats_registry();

/**
 * Logging functions
 */

void stats_start(StatisticsLogger* logger);
void stats_log_event(StatisticsLogger* logger);
void stats_stop_log(StatisticsLogger* logger, unsigned long repetitions);
void stats_stop_log_bytes(StatisticsLogger* logger, unsigned long bytes);

/**
 * Time series logging
 */

void start_time_series_logging();
void log_time_series_chunk(); /* Returns the chunk number for the time series. */

TimeSeriesLogger* register_time_series(const char* name);
void log_time_series_add_chunk_data(TimeSeriesLogger*, double value);

/**
 * Handles to the statistics loggers are stored centrally to make outputting
 * data straightforward.
 */

void report_stats();

JSON* report_stats_json();



/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_stats_H */
