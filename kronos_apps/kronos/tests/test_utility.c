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

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif

#include "common/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

void test_next_power_of_2() {

    assert(next_power_of_2(0) == 1);

    assert(next_power_of_2(1) == 1);
    assert(next_power_of_2(2) == 2);
    assert(next_power_of_2(3) == 4);
    assert(next_power_of_2(4) == 4);

    assert(next_power_of_2(512) == 512);
    assert(next_power_of_2(513) == 1024);

    /* Test the bits that require > 32 bits */

    assert(next_power_of_2(0x100000000) == 0x100000000);
    assert(next_power_of_2(0x100000001) == 0x200000000);
}

void test_is_power_of_2() {

    assert(is_power_of_2(1));
    assert(is_power_of_2(2));
    assert(is_power_of_2(4));
    assert(is_power_of_2(8));
    assert(is_power_of_2(1024));
    assert(is_power_of_2(0x100000000));
    assert(is_power_of_2(0x40000000000));

    assert(!is_power_of_2(3));
    assert(!is_power_of_2(5));
    assert(!is_power_of_2(11));
    assert(!is_power_of_2(1025));
    assert(!is_power_of_2(0xFFFFFFFF));
    assert(!is_power_of_2(0x40000000001));

    assert(!is_power_of_2(0));
    assert(!is_power_of_2(-2));
}

/* ------------------------------------------------------------------------------------------------------------------ */

int main() {
    test_next_power_of_2();
    test_is_power_of_2();
    return 0;
}
