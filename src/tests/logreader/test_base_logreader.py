#!/usr/bin/env python
import types
import unittest
import tempfile
import shutil
import os

from exceptions_iows import ConfigurationError
from jobs import IngestedJob
from logreader.base import LogReader
from logreader.dataset import IngestedDataSet


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
        self.assertIsNone(lr.file_pattern)
        self.assertFalse(lr.recursive)
        self.assertIsNone(lr.label_method)
        self.assertEqual(lr.job_class, IngestedJob)
        self.assertEqual(lr.log_type_name, '(Unknown)')
        self.assertEqual(lr.pool_readers, 10)

        # Test that we can override with the things we provide
        lr = LogReader('fake-path', recursive=True, file_pattern="*.pattern", label_method="directory", pool_readers=99)
        self.assertEqual(lr.path, 'fake-path')
        self.assertEqual(lr.file_pattern, "*.pattern")
        self.assertTrue(lr.recursive)
        self.assertEqual(lr.label_method, "directory")
        self.assertEqual(lr.pool_readers, 99)

        # Test that file_pattern is using the class default if not overridden.
        class LogReaderSubclass(LogReader):
            file_pattern = "default_pattern"
            label_method = "directory"
            recursive = True

        lr = LogReaderSubclass('fake-path')
        self.assertEqual(lr.file_pattern, "default_pattern")
        self.assertEqual(lr.label_method, "directory")
        self.assertTrue(lr.recursive)

        # Check that the validity of the label_method is tested
        self.assertRaises(ConfigurationError, lambda: LogReader('fake-path', label_method="invalid"))

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
            self.assertIsInstance(lr.logfiles(), types.GeneratorType)
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
            self.assertIsInstance(lr.logfiles(), types.GeneratorType)
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
            self.assertIsInstance(lr.logfiles(), types.GeneratorType)
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
            self.assertIsInstance(lr.logfiles(), types.GeneratorType)
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
            self.assertIsInstance(lr.logfiles(), types.GeneratorType)
            files = sorted([x for x in lr.logfiles()])

            self.assertEqual(files, sorted([os.path.join(directory, fn) for fn in target_files]))

        finally:
            shutil.rmtree(directory)

    def test_read_log_stubbed(self):
        """
        The actual read_log function needs to be implemented by the derived types
        """
        lr = LogReader('test-path')

        self.assertRaises(NotImplementedError, lambda: lr.read_log('fake-filename', None))

    def test_suggested_labels(self):

        lr = LogReader('test-path')
        self.assertIsNone(lr.suggest_label("a/file/path.test"))

        lr = LogReader('a', label_method="directory")
        self.assertEqual(lr.suggest_label("a/file/path.test"), "file")

    def test_read_logs(self):
        """
        Should return a generator of the output of read_log depending on the results of logfiles.
        """
        class LogReaderLocal(LogReader):
            def logfiles(self):
                for file in ['file1', 'file2', 'file3', 'file4']:
                    yield file

            def read_log(self, filename, suggested_label):
                return [{'name': filename, 'label': suggested_label}]

        lr = LogReaderLocal('test-path')

        logs = lr.read_logs()

        imax = 0
        for i, l in enumerate(logs):
            self.assertIsInstance(l, dict)
            self.assertEqual(l['name'], 'file{}'.format(i+1))
            imax = i

        self.assertEqual(imax, 3)


if __name__ == "__main__":
    unittest.main()
