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

#include <stdarg.h>
#include <stdio.h>

#include "kronos/global_config.h"
#include "kronos/trace.h"
#include "common/utility.h"


/* ------------------------------------------------------------------------------------------------------------------ */

void printf_trace(const char* fn_id, const char* fmt, ...) {

    /*clock_t now;
    time_t now2;*/
    double now;
    va_list args;

    const GlobalConfig* global_conf = global_config_instance();

    if (global_conf->enable_trace) {

        /*now = clock();
        now2 = time(NULL);

        printf("[%f : %li][%s]: ", ((double)(now - global_conf->start_time)) / CLOCKS_PER_SEC,
               (long)difftime(now2, global_conf->start_time2), fn_id);*/

        now = take_time();
        printf("[%f][%s]: ", now - global_conf->start_time3, fn_id);

        va_start(args, fmt);
        vprintf(fmt, args);
        va_end(args);

        printf("\n");
    }
}
