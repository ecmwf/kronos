#!/usr/bin/env python
import datetime
import os
import shutil
import sys
import types
import unittest

# Ensure imports work both in installation, and git, environments
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'kronos_py'))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'bin'))

from kronos.executor import executor
from kronos.executor.global_config import global_config
# from kronos.executor.job_classes import base_job

from kronos.io.schedule_format import ScheduleFormat

from testutils import scratch_tmpdir


class ExecutorTests(unittest.TestCase):

    def setUp(self):
        """
        The executor needs some base settings. Provide them (with a modulating job directory)
        """
        self.base_config = {
            'procs_per_node': 1,
            'job_dir': scratch_tmpdir(),
            'read_cache': scratch_tmpdir(),
            'job_dir_shared': scratch_tmpdir(),
            'coordinator_binary': 'invalid-binary'
        }

    def tearDown(self):
        """
        We may have created temporary files in the job dir, ensure that they disappear.
        """
        if os.path.exists(self.base_config['job_dir']):
            shutil.rmtree(self.base_config['job_dir'])
        if os.path.exists(self.base_config['read_cache']):
            shutil.rmtree(self.base_config['read_cache'])
        if os.path.exists(self.base_config['job_dir_shared']):
            shutil.rmtree(self.base_config['job_dir_shared'])

    def test_wait_until(self):
        """
        Test that the wait function waits until the specified delay after it is first called.
        """
        now = datetime.datetime.now()

        e = executor.Executor(self.base_config, ScheduleFormat(sa_data_json=[]))

        e.wait_until(-5)
        self.assertEqual((datetime.datetime.now() - now).seconds, 0)
        e.wait_until(1)
        self.assertEqual((datetime.datetime.now() - now).seconds, 1)
        e.wait_until(3)
        self.assertEqual((datetime.datetime.now() - now).seconds, 3)
        e.wait_until(3)
        self.assertEqual((datetime.datetime.now() - now).seconds, 3)

    def test_job_iterator_config(self):

        job_list = [ {'name': 'a'}, {'name': 'b'}, {'name': 'c'} ]

        e = executor.Executor(self.base_config, ScheduleFormat(sa_data_json=job_list))

        jobs = e.job_iterator()
        self.assertIsInstance(jobs, types.GeneratorType)
        jobs = list(jobs)

        self.assertEqual(jobs, job_list)

    def test_job_iterator_invalid(self):
        """
        What happens if we pass simply invalid JSON to the job iterator...
        """
        ## Disabled, as this is now tested in the ScheduleFormat constructor.
        # e = executor.Executor(self.base_config, ScheduleFormat(sa_data_json=[{"test": "oops"}]))

        # jobs = e.job_iterator()
        # self.assertIsInstance(jobs, types.GeneratorType)
        # self.assertRaises(AssertionError, lambda: list(jobs))

        # shutil.rmtree(self.base_config['job_dir'])
        e = executor.Executor(self.base_config, ScheduleFormat(sa_data_json=[1, 2, 3]))

        jobs = e.job_iterator()
        self.assertIsInstance(jobs, types.GeneratorType)
        self.assertRaises(AssertionError, lambda: list(jobs))

    def test_required_options(self):
        """
        Some of the options are required.
        """
        # Set ourselves into a temporary directory, so that we don't break anything
        tmp_cwd = os.getcwd()
        tmpdir = scratch_tmpdir()
        rundir = os.path.join(tmpdir, 'run')
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        # An absolutely minimal configuration
        config_minimal = {
            'procs_per_node': 123,
            'read_cache': self.base_config['read_cache'],
            'coordinator_binary': 'a-binary'
        }

        try:

            config_fail = config_minimal.copy()
            del config_fail['procs_per_node']
            self.assertRaises(KeyError, lambda: executor.Executor(config_fail, global_config))

            if os.path.exists(rundir):
                shutil.rmtree(rundir)
            config_fail = config_minimal.copy()
            del config_fail['read_cache']
            self.assertRaises(KeyError, lambda: executor.Executor(config_fail, global_config))

            # Test that extra parameters cause a failure
            if os.path.exists(rundir):
                shutil.rmtree(rundir)
            config_fail = config_minimal.copy()
            config_fail['extra_parameter'] = "boo"
            self.assertRaises(executor.Executor.InvalidParameter, lambda: executor.Executor(config_fail, global_config))

            # And check that the minimal config works!
            if os.path.exists(rundir):
                shutil.rmtree(rundir)
            executor.Executor(config_minimal, ScheduleFormat(sa_data_json=[]))

        finally:
            # Just in case, clear everything up
            os.chdir(tmp_cwd)
            shutil.rmtree(tmpdir)

    def test_defaults(self):
        """
        Test that, when initialised, the Executor picks up the correct defaults
        """
        # Set ourselves into a temporary directory, so that we don't break anything
        tmp_cwd = os.getcwd()
        tmpdir = scratch_tmpdir()
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        try:

            # An absolutely minimal configuration
            config_minimal = {
                'procs_per_node': 123,
                'read_cache': self.base_config['read_cache'],
                'coordinator_binary': 'a-binary'
            }

            # And initialise an executor. See what happens!
            e = executor.Executor(config_minimal, global_config)

            expected_job_path = os.path.join(tmpdir, "run")
            self.assertEqual(e.job_dir, expected_job_path)

            self.assertEqual(e.procs_per_node, 123)
            self.assertIsNone(e.initial_time)
            self.assertFalse(e.enable_ipm)
            self.assertEqual(e.read_cache_path, self.base_config['read_cache'])

        finally:
            # Just in case, clear everything up
            os.chdir(tmp_cwd)
            shutil.rmtree(tmpdir)

    def test_configurability(self):
        """
        Test that, when initialised, the Executor is appopriately sensitive to all the
        specified configuration options.
        """
        # Set ourselves into a temporary directory, so that we don't break anything
        tmp_cwd = os.getcwd()
        tmpdir = scratch_tmpdir()
        tmpdir2 = scratch_tmpdir()
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        try:

            # An absolutely minimal configuration
            config_minimal = {
                'procs_per_node': 666,
                'read_cache': self.base_config['read_cache'],
                'job_class': 'slurm',
                'job_dir': tmpdir2,
                'coordinator_binary': 'a-binary',
                'enable_ipm': True
            }

            # And initialise an executor. See what happens!
            e = executor.Executor(config_minimal, global_config)

            self.assertEqual(e.job_dir, tmpdir2)
            self.assertTrue(os.path.exists(tmpdir2) and os.path.isdir(tmpdir2))

            self.assertEqual(e.coordinator_binary, 'a-binary')
            self.assertEqual(e.procs_per_node, 666)
            self.assertIsNone(e.initial_time)  # Not configurable
            self.assertTrue(e.enable_ipm)
            self.assertEqual(e.read_cache_path, self.base_config['read_cache'])

        finally:
            # Just in case, clear everything up
            os.chdir(tmp_cwd)
            shutil.rmtree(tmpdir)

            if os.path.exists(tmpdir2):
                shutil.rmtree(tmpdir2)


if __name__ == "__main__":
    unittest.main()
