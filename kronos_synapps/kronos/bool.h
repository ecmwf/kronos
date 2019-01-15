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


#ifndef kronos_bool_H
#define kronos_bool_H


/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * It would be nicer to use stdbool.h, but that requires C99 support. To be as general
 * as possible...
 */

typedef enum {
    false,
    true
} bool;


/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_bool_H */
