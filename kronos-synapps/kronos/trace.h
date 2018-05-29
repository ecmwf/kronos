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


#ifndef kronos_trace_H
#define kronos_trace_H

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * printf-style trace macro
 */

void printf_trace(const char* fn_id, const char* fmt, ...);

/*#define CURRENT_FUNCTION __func__*/
#define CURRENT_FUNCTION __FUNCTION__

#define TRACE() printf_trace(CURRENT_FUNCTION, "ENTER")
#define TRACE1(x) printf_trace(CURRENT_FUNCTION, x)
#define TRACE2(x, y) printf_trace(CURRENT_FUNCTION, x, y)
#define TRACE3(x, y, z) printf_trace(CURRENT_FUNCTION, x, y, z)
#define TRACE4(w, x, y, z) printf_trace(CURRENT_FUNCTION, w, x, y, z)

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_trace_H */

