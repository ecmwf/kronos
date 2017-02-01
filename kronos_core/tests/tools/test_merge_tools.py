#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import unittest

from kronos_tools.merge import min_not_none, max_not_none


class ModelJobTest(unittest.TestCase):

    def test_min_not_none(self):

        self.assertIsNone(min_not_none())
        self.assertIsNone(min_not_none(None, None, None))
        self.assertIsNone(min_not_none(None, None, None))
        self.assertEqual(min_not_none(None, None, 1), 1)
        self.assertEqual(min_not_none(1, None, None), 1)
        self.assertEqual(min_not_none(3, 1, None), 1)
        self.assertEqual(min_not_none(None, 1, 3), 1)
        self.assertEqual(min_not_none(1, 3), 1)

    def test_max_not_none(self):

        self.assertIsNone(max_not_none())
        self.assertIsNone(max_not_none(None, None, None))
        self.assertIsNone(max_not_none(None, None, None))
        self.assertEqual(max_not_none(None, None, 1), 1)
        self.assertEqual(max_not_none(1, None, None), 1)
        self.assertEqual(max_not_none(3, 1, None), 3)
        self.assertEqual(max_not_none(None, 1, 3), 3)
        self.assertEqual(max_not_none(1, 3), 3)

if __name__ == "__main__":
    unittest.main()
