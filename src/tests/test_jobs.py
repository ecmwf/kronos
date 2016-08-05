#!/usr/bin/env python
import unittest

from jobs import ModelJob


class ModelJobTest(unittest.TestCase):

    def test_initialisation(self):
        self.assertFalse(True)

    def test_is_valid(self):
        """
        There are some things that are required. Check these things here!
        """
        job = ModelJob()
        self.assertFalse(job.is_valid())

        # If all of the required arguments are supplied, this should result in a valid job
        valid_args = {
            'time_start': 0,
            'ncpus': 1,
            'nnodes': 1
        }
        self.assertTrue(ModelJob(**valid_args).is_valid())

        # If any of the supplied arguments are missing, this should invalidate things
        for k in valid_args.keys():
            invalid_args = valid_args.copy()
            del invalid_args[k]
            self.assertTrue(ModelJob(**valid_args).is_valid())


class IngestedJobTest(unittest.TestCase):

    def test_initialisation(self):
        """
        Check that we have sensible (overridable) defaults. N.b. inherits from LogReader
        """
        self.assertFalse(True)


if __name__ == "__main__":
    unittest.main()
