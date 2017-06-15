#!/usr/bin/env python

import os
import shutil
import sys
import unittest

# Ensure imports work both in installation, and git, environments
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'kronos_py'))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'bin'))

from kronos.executor import generate_read_files
from kronos.executor.global_config import global_config

from testutils import scratch_tmpdir


class ReadFileGenerationTests(unittest.TestCase):

    def test_human_readable_bytes(self):
        """
        This is a test case!!!
        """
        self.assertEqual(generate_read_files.human_readable_bytes(123.4567), "123.457 B")
        self.assertEqual(generate_read_files.human_readable_bytes(123456.7890), "120.563 KiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789), "117.738 MiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012), "114.978 GiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345), "112.283 TiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345678), "109.652 PiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345678901), "107.082 EiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345678901234), "104.572 ZiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345678901234567), "102.121 YiB")
        self.assertEqual(generate_read_files.human_readable_bytes(123456789012345678901234567890), "102121.062 YiB")

    def test_enumerate_cache_files(self):
        """
        Given inputs, do we get the correct list back?
        """
        path = "test-path"
        global_config['read_file_multiplicity'] = 2
        global_config['read_file_size_min'] = 4
        global_config['read_file_size_max'] = 6

        output = list(generate_read_files.enumerate_cache_files(path))

        self.assertEqual(len(output), 6)

        # Only generate files for he biggest size since NEX-113
        #for (fn, sz), expected_sz in zip(output, [16, 16, 32, 32, 64, 64]):
        for (fn, sz), expected_sz in zip(output, [16, 16, 32, 32, 64, 64]):
            self.assertEqual(sz, expected_sz)
            self.assertEqual(fn[:-2], "test-path/read-cache-{}".format(sz))

    def test_generate_read_cache(self):
        """
        Test that the generate routine generates what we expect
        """
        # This will throw if TMPDIR doesn't exist
        path = scratch_tmpdir()
        global_config['read_file_multiplicity'] = 3
        global_config['read_file_size_min'] = 3
        global_config['read_file_size_max'] = 5

        try:
            generate_read_files.generate_read_cache(path)

            self.assertTrue(os.path.exists(path) and os.path.isdir(path))

            # Only generate files for he biggest size since NEX-113
            #for sz in [8, 16, 32]:
            for sz in [32]:
                for cnt in range(3):
                    fn = os.path.join(path, "read-cache-{}-{}".format(sz, cnt))
                    print fn
                    self.assertTrue(os.path.exists(fn))
                    self.assertTrue(os.path.isfile(fn))
                    self.assertEqual(os.path.getsize(fn), sz)

        finally:
            # Ensure that things are cleaned up properly, whatever happens
            shutil.rmtree(path)

    def test_test_read_cache(self):
        """
        Test that the test routine tests what we expect
        """
        # This will throw if TMPDIR doesn't exist
        path = scratch_tmpdir()
        global_config['read_file_multiplicity'] = 3
        global_config['read_file_size_min'] = 3
        global_config['read_file_size_max'] = 5

        self.assertFalse(generate_read_files.test_read_cache(path))

        try:
            generate_read_files.generate_read_cache(path)

            self.assertTrue(generate_read_files.test_read_cache(path))

            # Move each of the required files somewhere else, and check that it causes failures.
            # Move in a file of the wrong size, and check that also fails. Then return it and
            # check that everything passes.
            tmpfile = os.path.join(path, "tmpfile")

            # Only generate files for he biggest size since NEX-113
            # for sz in [8, 16, 32]:
            for sz in [32]:
                for cnt in range(3):
                    fn = os.path.join(path, "read-cache-{}-{}".format(sz, cnt))

                    os.rename(fn, tmpfile)
                    self.assertFalse(generate_read_files.test_read_cache(path))

                    with open(fn, 'w') as f:
                        f.write('\0')
                    self.assertFalse(generate_read_files.test_read_cache(path))
                    os.remove(fn)

                    os.rename(tmpfile, fn)
                    self.assertTrue(generate_read_files.test_read_cache(path))

        finally:
            # Ensure that things are cleaned up properly, whatever happens
            shutil.rmtree(path)


if __name__ == "__main__":
    unittest.main()