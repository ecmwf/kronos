#!/usr/bin/env python
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
