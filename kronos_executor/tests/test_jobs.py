#!/usr/bin/env python
import json
import unittest
import sys
import os
import shutil

# Ensure imports work both in installation, and git, environments
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'kronos_py'))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'bin'))

from job_classes.base_job import BaseJob
from testutils import scratch_tmpdir


class ExecutorStub(object):
    pass


class ExecutorTests(unittest.TestCase):

    def test_stubbed_routines(self):

        job = BaseJob({}, ExecutorStub(), "fake-path")

        self.assertRaises(NotImplementedError, job.generate_internal)
        self.assertRaises(NotImplementedError, job.run)

    def test_start_delay(self):

        # Test default
        job = BaseJob({}, ExecutorStub(), "fake-path")
        self.assertEqual(job.start_delay, 0)

        # Test that this is overridden correctly from the job config
        job = BaseJob({ "start_delay": 123}, ExecutorStub(), "")
        self.assertEqual(job.start_delay, 123)

        # Test error handling
        self.assertRaises(TypeError, lambda: BaseJob({"start_delay": "invalid"}, ExecutorStub(), ""))

    def test_input_filename(self):
        """
        The input file for the job will be generated within the specified path
        """
        job = BaseJob({}, ExecutorStub(), "fake-path")
        self.assertEqual(job.path, "fake-path")
        self.assertEqual(job.input_file, "fake-path/input.json")

    def test_generate(self):
        """
        There is a generate wrapper, that should call generate_internal (which is implemented
        in the base classes), but also generate the input files, and create the relevant execution
        directories.
        """
        class TrialJob(BaseJob):
            def __init__(self, *args, **kwargs):
                self.called = False
                super(TrialJob, self).__init__(*args, **kwargs)

            def generate_internal(self):
                self.called = True

        path = scratch_tmpdir()
        try:

            config = {"test": "elem"}
            job = TrialJob(config, ExecutorStub(), path)
            self.assertFalse(job.called)
            self.assertFalse(os.path.exists(path))

            job.generate()

            self.assertTrue(job.called)
            self.assertTrue(os.path.exists(path))

            input_path = os.path.join(path, "input.json")
            self.assertTrue(os.path.exists(input_path))

            with open(input_path, "r") as f:
                loaded_json = json.load(f)
                self.assertEqual(loaded_json, config)

        finally:
            # Ensure cleanup of temporaries, whatever happens.
            if os.path.exists(path):
                shutil.rmtree(path)


if __name__ == "__main__":
    unittest.main()

