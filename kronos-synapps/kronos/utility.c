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

#include "utility.h"

#include <sys/time.h>
#include <assert.h>

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Return the first power of 2 that is >= a given number
 * See bithacks at: http://www.graphics.stanford.edu/~seander/bithacks.html
 */
long next_power_of_2(long num) {

    if (num == 0)
        return 1;

    num--;
    num |= num >> 1;
    num |= num >> 2;
    num |= num >> 4;
    num |= num >> 8;
    num |= num >> 16;
    if (sizeof(long) == 8) {
        num |= num >> 32;
    }
    num++;

    return num;
}

/*
 * Return true if the supplied number is a power of 2, otherwise false
 */
bool is_power_of_2(long num) {
    return (num > 0) && !(num & (num - 1));
}


/*
 * This routine exists only to "confuse" the compiler so that it prevents optimisations
 * being applied as would be expected
 *
 * --> Needed when 'wasting' cycles
 */
void dummy_deoptimise(void * array) {
    (void) array;
}


/*
 * Take the curretn wall time, in Unix epoch time, and return it as a double (in seconds)
 */
double take_time() {

    struct timeval t;
    int ret;

    ret = gettimeofday(&t, 0);
    assert(ret == 0);

    return (double)t.tv_sec + ((double)t.tv_usec / 1000000.);
}

/* ------------------------------------------------------------------------------------------------------------------ */
