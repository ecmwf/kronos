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

#include <string.h>
#include <stdlib.h>
#include <assert.h>

/* ------------------------------------------------------------------------------------------------------------------ */

typedef struct StatisticsRegistry {

    StatisticsLogger* loggers;

} StatisticsRegistry;


static StatisticsRegistry* stats_instance() {

    static StatisticsRegistry registry;

    bool initialised = false;
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


StatisticsLogger* create_stats_logger(const char *name) {

    StatisticsRegistry* registry = stats_instance();

    /* Ensure that an existing logger does not exist with this name */
    StatisticsLogger* logger = registry->loggers;
    while (logger != 0) {
        assert(logger->name != 0 && strcmp(name, logger->name) != 0);
        logger = logger->next;
    }

    logger = malloc(sizeof(StatisticsLogger));
    logger->name = strdup(name);

    /* Insert into the linked list */
    logger->next = registry->loggers;
    registry->loggers = logger;

    return logger;
}


void report_stats(FILE* fp) {}

JSON* stats_json() {}

/* ------------------------------------------------------------------------------------------------------------------ */
