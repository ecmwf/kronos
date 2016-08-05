#!/usr/bin/env python
import unittest

from jobs import ModelJob


class ModelJobTest(unittest.TetsCase):

    def test_initialisation(self):
        self.assertFalse(True)


class IngestedJobTest(unittest.TestCase):

    def test_initialisation(self):
        """
        Check that we have sensible (overridable) defaults. N.b. inherits from LogReader
        """
        self.assertFalse(True)


if __name__ == "__main__":
    unittest.main()
