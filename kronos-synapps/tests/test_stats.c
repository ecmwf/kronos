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

    stats_start(logger1);
    usleep(20000);
    stats_stop_log(logger1);

    assert(logger1->count == 1);
    assert(logger1->sumTimes >= 0.0198 && logger1->sumTimes <= 0.0202);
    assert(logger1->sumTimesSquared >= 0.00039 && logger1->sumTimesSquared <= 0.00041);

    stats_start(logger1);
    usleep(40000);
    stats_stop_log(logger1);

    assert(logger1->count == 2);
    assert(logger1->sumTimes >= 0.0598 && logger1->sumTimes <= 0.0603);
    assert(logger1->sumTimesSquared >= 0.0019 && logger1->sumTimesSquared <= 0.0021);

    /* And clear up */

    free_stats_registry();
    assert_logger_count(0);
}

static void test_timers_with_bytes() {

    StatisticsLogger* logger1;

    assert_logger_count(0);

    logger1 = create_stats_times_logger("logger1");

    stats_start(logger1);
    usleep(20000);
    stats_stop_log(logger1);

    assert(logger1->count == 1);
    assert(logger1->sumTimes >= 0.0198 && logger1->sumTimes <= 0.0202);
    assert(logger1->sumTimesSquared >= 0.00039 && logger1->sumTimesSquared <= 0.00041);

    stats_start(logger1);
    usleep(40000);
    stats_stop_log(logger1);

    assert(logger1->count == 2);
    assert(logger1->sumTimes >= 0.0598 && logger1->sumTimes <= 0.0603);
    assert(logger1->sumTimesSquared >= 0.0019 && logger1->sumTimesSquared <= 0.0021);

    /* And clear up */

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


    clean_global_config();
    return 0;
}
