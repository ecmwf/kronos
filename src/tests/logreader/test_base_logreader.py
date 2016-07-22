#!/usr/bin/env python
import types
import unittest
import tempfile
import shutil
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

    def test_logfiles_nonexistent(self):
        """
        If the specified path does not exist, then an exception should be raised when trying to iterate
        through the available files.
        """
        self.assertRaises(IOError, LogReader('fake-path').logfiles().next)

    def test_logfiles_single_file(self):
        """
        If the path provided is a single file, then it should return that.
        """
        with tempfile.NamedTemporaryFile() as f:

            lr = LogReader(f.name)
            assert isinstance(lr.logfiles(), types.GeneratorType)
            files = [x for x in lr.logfiles()]

            self.assertEqual(files, [f.name])

    def test_logfiles_directory(self):
        """
        We should enumerate the matching files from the specified path, without recursively
        descending into subdirectories
        """
        dir_list = ['dir1', 'dir2/dir3']

        file_list = [
            'file1.log',
            'file2.log',
            'dir1/file3.log',
            'dir2/dir3/file4.log',
            'other1',
            'other2',
            'dir1/other3'
        ]

        target_files = [
            'file1.log',
            'file2.log',
            'other1',
            'other2'
        ]

        # Create a temporary directory. try/finally ensures that it _WILL_ be deleted
        directory = tempfile.mkdtemp()
        try:
            for d in dir_list:
                os.makedirs(os.path.join(directory, d))
            for fn in file_list:
                # Create an (empty) file
                open(os.path.join(directory, fn), 'a').close()

            lr = LogReader(directory)
            assert isinstance(lr.logfiles(), types.GeneratorType)
            files = sorted([x for x in lr.logfiles()])

            self.assertEqual(files, sorted([os.path.join(directory, fn) for fn in target_files]))

        finally:
            shutil.rmtree(directory)

    def test_logfiles_directory_pattern(self):
        """
        We should enumerate the matching files from the specified path, without recursively
        descending into subdirectories
        """
        dir_list = ['dir1', 'dir2/dir3']

        file_list = [
            'file1.log',
            'file2.log',
            'dir1/file3.log',
            'dir2/dir3/file4.log',
            'other1',
            'other2',
            'dir1/other3'
        ]

        target_files = [
            'file1.log',
            'file2.log',
        ]

        # Create a temporary directory. try/finally ensures that it _WILL_ be deleted
        directory = tempfile.mkdtemp()
        try:
            for d in dir_list:
                os.makedirs(os.path.join(directory, d))
            for fn in file_list:
                # Create an (empty) file
                open(os.path.join(directory, fn), 'a').close()

            lr = LogReader(directory, file_pattern="*.log")
            assert isinstance(lr.logfiles(), types.GeneratorType)
            files = sorted([x for x in lr.logfiles()])

            self.assertEqual(files, sorted([os.path.join(directory, fn) for fn in target_files]))

        finally:
            shutil.rmtree(directory)

    def test_logfiles_directory_recursive(self):
        """
        We should enumerate the matching files from the specified path, without recursively
        descending into subdirectories
        """
        dir_list = ['dir1', 'dir2/dir3']

        file_list = [
            'file1.log',
            'file2.log',
            'dir1/file3.log',
            'dir2/dir3/file4.log',
            'other1',
            'other2',
            'dir1/other3'
        ]

        target_files = file_list

        # Create a temporary directory. try/finally ensures that it _WILL_ be deleted
        directory = tempfile.mkdtemp()
        try:
            for d in dir_list:
                os.makedirs(os.path.join(directory, d))
            for fn in file_list:
                # Create an (empty) file
                open(os.path.join(directory, fn), 'a').close()

            lr = LogReader(directory, recursive=True)
            assert isinstance(lr.logfiles(), types.GeneratorType)
            files = sorted([x for x in lr.logfiles()])

            self.assertEqual(files, sorted([os.path.join(directory, fn) for fn in target_files]))

        finally:
            shutil.rmtree(directory)

    def test_logfiles_directory_recursive_pattern(self):
        """
        We should enumerate the matching files from the specified path, without recursively
        descending into subdirectories
        """
        dir_list = ['dir1', 'dir2/dir3']

        file_list = [
            'file1.log',
            'file2.log',
            'dir1/file3.log',
            'dir2/dir3/file4.log',
            'other1',
            'other2',
            'dir1/other3'
        ]

        target_files  = [
            'file1.log',
            'file2.log',
            'dir1/file3.log',
            'dir2/dir3/file4.log'
        ]

        # Create a temporary directory. try/finally ensures that it _WILL_ be deleted
        directory = tempfile.mkdtemp()
        try:
            for d in dir_list:
                os.makedirs(os.path.join(directory, d))
            for fn in file_list:
                # Create an (empty) file
                open(os.path.join(directory, fn), 'a').close()

            lr = LogReader(directory, recursive=True, file_pattern="*.log")
            assert isinstance(lr.logfiles(), types.GeneratorType)
            files = sorted([x for x in lr.logfiles()])

            self.assertEqual(files, sorted([os.path.join(directory, fn) for fn in target_files]))

        finally:
            shutil.rmtree(directory)

if __name__ == "__main__":
    unittest.main()
