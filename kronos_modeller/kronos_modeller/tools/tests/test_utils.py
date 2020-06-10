#!/usr/bin/env python

# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest

from kronos_executor.tools import add_value_to_sublist, calc_histogram


class ProfileFormatTest(unittest.TestCase):

    def test_add_to_sublist(self):
        """
        Check that the schema method is correctly loading something that looks schema-ish
        """

        _test_list = [1, 4, 6, 4, 3, 56, 67, 7, 9]
        _test_list_out = add_value_to_sublist(_test_list, 3, 6, 0.999)

        self.assertEqual(id(_test_list_out), id(_test_list))
        self.assertEqual(_test_list_out, [1, 4, 6, 4.999, 3.999, 56.999, 67, 7, 9])

        # tests on the indices
        self.assertRaises(AssertionError, lambda: add_value_to_sublist(_test_list, -1, 6, 0.999))
        self.assertRaises(AssertionError, lambda: add_value_to_sublist(_test_list, 0, 12, 0.999))
        self.assertRaises(AssertionError, lambda: add_value_to_sublist(_test_list, -10, -8, 0.999))
        self.assertRaises(AssertionError, lambda: add_value_to_sublist(_test_list, 0.66, 0.34, 0.999))
        self.assertRaises(AssertionError, lambda: add_value_to_sublist(_test_list, 0.22, 0.88, 0.999))

        # tests on an empty list
        self.assertRaises(AssertionError, lambda: add_value_to_sublist([], 0, 12, 0.999))

    def test_calc_histogram(self):

        # number 0 to 9 in 2 intervals
        self.assertEqual(calc_histogram(list(range(10)), 2), ([0.0, 4.5, 9.0], [5, 5]))

        # some numbers from 0 to 10 in 5 intervals
        self.assertEqual(calc_histogram([10, 0, 5.5, 8, 2.1], 5), ([0., 2., 4., 6., 8., 10], [1, 1, 1, 0, 2]))
