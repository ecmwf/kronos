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


#ifndef kronos_utility_H
#define kronos_utility_H

#include "bool.h"

/* ------------------------------------------------------------------------------------------------------------------ */

long next_power_of_2(long num);

bool is_power_of_2(long num);

void dummy_deoptimise(void * array);

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_utility_H */

