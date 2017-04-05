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

#include <stdio.h>

typedef struct JSON JSON;

/* ------------------------------------------------------------------------------------------------------------------ */

/* A statistics logger that can be attached to a kernel */

typedef struct StatisticsLogger {

    char* name;

    struct StatisticsLogger* next;

} StatisticsLogger;


/**
 * Obtain access to a statistics logger for the specified name.
 * @note The receiver does not get _ownership_ of the stats logger, and should
 *       not attempt to clean it up.
 */

StatisticsLogger* create_stats_logger(const char* name);
void free_stats_registry();

/**
 * Handles to the statistics loggers are stored centrally to make outputting
 * data straightforward.
 */

void report_stats(FILE* fp);

JSON* stats_json();



/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_stats_H */