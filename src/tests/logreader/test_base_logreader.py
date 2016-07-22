#!/usr/bin/env python
import unittest
import os

from logreader.base import LogReader


class BaseLogReaderTest(unittest.TestCase):

    def test_initialisation(self):
        """
        The configuration object should have some sane defaults
        """

        # The path is required
        self.assertRaises(TypeError, lambda: LogReader())

        # Test the available defaults
        lr = LogReader('fake-path')
        self.assertEqual(lr.path, 'fake-path')
        self.assertIsNone(lr.dataset_class)
        self.assertIsNone(lr.file_pattern)
        self.assertFalse(lr.recursive)

        # Test that we can override with the things we provide
        lr = LogReader('fake-path', recursive=True, file_pattern="*.pattern")
        self.assertEqual(lr.path, 'fake-path')
        self.assertIsNone(lr.dataset_class)
        self.assertEqual(lr.file_pattern, "*.pattern")
        self.assertTrue(lr.recursive)

        # Test that file_pattern is using the class default if not overridden.
        class LogReaderSubclass(LogReader):
            file_pattern = "default_pattern"

        lr = LogReaderSubclass('fake-path')
        self.assertEqual(lr.file_pattern, "default_pattern")

if __name__ == "__main__":
    unittest.main()
