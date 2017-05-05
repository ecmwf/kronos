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
 * @date Jun 2016
 * @author Simon Smart
 */

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif
#include <stdlib.h>
#include <math.h>

#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/stats.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void assert_logger_count(int count) {

    JSON* report = report_stats_json();

    const JSON* stats = json_object_get(report, "stats");
    assert(stats != 0);
    assert(json_is_object(stats));

    assert(json_object_count(stats) == count);

    free_json(report);
}


static void test_create_loggers() {

    StatisticsLogger* logger1;
    StatisticsLogger* logger2;
    int test_val; /* Avoid warnings with GNU + pedantic */

    assert_logger_count(0);

    logger1 = create_stats_times_logger("logger1");

    test_val = strcmp("logger1", logger1->name);
    assert(test_val == 0);
    assert(logger1->logTimes);
    assert(!logger1->logBytes);

    assert(logger1->count == 0);
    assert(logger1->sumTimes == 0);
    assert(logger1->sumTimesSquared == 0);

    assert(logger1->next == 0);

    assert_logger_count(1);

    logger2 = create_stats_times_bytes_logger("logger2");

    assert(logger2 != logger1);

    test_val = strcmp("logger2", logger2->name);
    assert(test_val == 0);
    assert(logger2->logTimes);
    assert(logger2->logBytes);

    assert(logger2->count == 0);
    assert(logger2->sumBytes == 0);
    assert(logger2->sumBytesSquared == 0);
    assert(logger2->sumTimes == 0);
    assert(logger2->sumTimesSquared == 0);

    assert(logger2->next == logger1);

    assert_logger_count(2);

    /* And clean up */

    free_stats_registry();
    assert_logger_count(0);
}


static void test_timers() {

    StatisticsLogger* logger1;

    assert_logger_count(0);

    logger1 = create_stats_times_logger("logger1");
    assert_logger_count(1);

    stats_start(logger1);
    usleep(20000);
    stats_stop_log(logger1, 1);

    assert(logger1->count == 1);
    assert(fabs(logger1->sumTimes - 0.02) < 0.0002);
    assert(fabs(logger1->sumTimesSquared - 0.0004) < 0.00001);

    stats_start(logger1);
    usleep(40000);
    stats_stop_log(logger1, 1);

    assert(logger1->count == 2);
    assert(fabs(logger1->sumTimes - 0.06) < 0.0003);
    assert(fabs(logger1->sumTimesSquared - 0.002) < 0.0001);

    /* And clear up */

    free_stats_registry();
    assert_logger_count(0);
}

static void test_timers_with_bytes() {

    StatisticsLogger* logger1;

    assert_logger_count(0);

    logger1 = create_stats_times_bytes_logger("logger1");
    assert_logger_count(1);

    stats_start(logger1);
    usleep(20000);
    stats_stop_log_bytes(logger1, 6666);

    assert(logger1->count == 1);
    assert(fabs(logger1->sumTimes - 0.02) < 0.0002);
    assert(fabs(logger1->sumTimesSquared - 0.0004) < 0.00001);
    assert(logger1->sumBytes == 6666);
    assert(logger1->sumBytesSquared == 44435556);

    stats_start(logger1);
    usleep(40000);
    stats_stop_log_bytes(logger1, 7777);

    assert(logger1->count == 2);
    assert(fabs(logger1->sumTimes - 0.06) < 0.0003);
    assert(fabs(logger1->sumTimesSquared - 0.002) < 0.0001);
    assert(logger1->sumBytes == 14443);
    assert(logger1->sumBytesSquared == 104917285);

    /* And clear up */

    free_stats_registry();
    assert_logger_count(0);
}

static void test_logger_json() {

    StatisticsLogger* logger1;
    JSON* report;
    const JSON* stats;
    const JSON* json_logger;
    long tmpi;
    double tmpf;

    assert_logger_count(0);

    logger1 = create_stats_times_bytes_logger("logger1");
    assert_logger_count(1);

    stats_start(logger1);
    usleep(20000);
    stats_stop_log_bytes(logger1, 6666);
    stats_start(logger1);
    usleep(40000);
    stats_stop_log_bytes(logger1, 7777);

    /* Get hold of a json that describes this logger */

    report = report_stats_json();
    stats = json_object_get(report, "stats");

    assert(stats != 0);
    assert(json_is_object(stats));
    assert(json_object_count(stats) == 1);

    json_logger = json_object_get(stats, "logger1");
    assert(json_logger != 0);

    assert(json_object_get_integer(json_logger, "count", &tmpi) == 0);
    assert(tmpi == 2);

    assert(json_object_get_integer(json_logger, "bytes", &tmpi) == 0);
    assert(tmpi == 14443);

    assert(json_object_get_integer(json_logger, "sumSquaredBytes", &tmpi) == 0);
    assert(tmpi == 104917285);

    assert(json_object_get_double(json_logger, "averageBytes", &tmpf) == 0);
    assert(fabs(tmpf - 7221.5) < 1.0e-10);

    assert(json_object_get_double(json_logger, "stddevBytes", &tmpf) == 0);
    assert(fabs(tmpf - 555.5) < 1.0e-10);

    assert(json_object_get_double(json_logger, "elapsed", &tmpf) == 0);
    assert(fabs(tmpf - 0.06) < 0.001);

    assert(json_object_get_double(json_logger, "sumSquaredElapsed", &tmpf) == 0);
    assert(fabs(tmpf - 0.002) < 0.0001);

    assert(json_object_get_double(json_logger, "averageElapsed", &tmpf) == 0);
    assert(fabs(tmpf - 0.03) < 0.005);

    assert(json_object_get_double(json_logger, "stddevElapsed", &tmpf) == 0);
    assert(fabs(tmpf - 0.01) < 1.0e-4);

    free_json(report);

    /* And clear up */

    free_stats_registry();
    assert_logger_count(0);
}

/* -----------------------------------------------------
 * Time series functionality */


static void assert_ts_logger_count(int count) {

    JSON* report = report_stats_json();

    const JSON* ts = json_object_get(report, "time_series");
    assert(ts != 0);
    assert(json_is_object(ts));

    /* Add one to the count, as there will always be a durations bit */
    assert(json_object_count(ts) == (count + 1));

    free_json(report);
}


static void test_create_ts_loggers() {

    assert_ts_logger_count(0);

    TimeSeriesLogger* logger1;
    TimeSeriesLogger* logger2;

    int test_val; /* Avoid warnings with GNU + pedantic */

    logger1 = register_time_series("ts-1");

    test_val = strcmp("ts-1", logger1->name);
    assert(test_val == 0);
    assert(logger1->chunks == 0);
    assert(logger1->count == 0);
    assert(logger1->next == 0);

    assert_ts_logger_count(1);

    logger2 = register_time_series("ts-2");

    test_val = strcmp("ts-2", logger2->name);
    assert(test_val == 0);
    assert(logger2->chunks == 0);
    assert(logger2->count == 0);
    assert(logger2->next == logger1);

    assert_ts_logger_count(2);

    /* And clean up */

    free_stats_registry();
    assert_logger_count(0);
}


/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    init_global_config(null_json(), 0, NULL);

    /* The number of MPI threads will impact things... */
    assert(global_config_instance()->nprocs == 1);
    assert(global_config_instance()->mpi_rank == 0);

    test_create_loggers();
    test_timers();
    test_timers_with_bytes();
    test_logger_json();

    test_create_ts_loggers();


    clean_global_config();
    return 0;
}
