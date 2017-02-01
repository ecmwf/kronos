#!/usr/bin/env python
import json
import types
import unittest
import sys
import os
import datetime
import shutil

# Ensure imports work both in installation, and git, environments
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'kronos_py'))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'bin'))

from kronos_executor import executor
from kronos_executor.global_config import global_config
from testutils import scratch_tmpdir
from kronos_executor.job_classes import base_job


class ExecutorTests(unittest.TestCase):

    def setUp(self):
        """
        The executor needs some base settings. Provide them (with a modulating job directory)
        """
        self.base_config = {
            'procs_per_node': 1,
            'job_dir': scratch_tmpdir(),
            'read_cache': scratch_tmpdir()
        }

    def tearDown(self):
        """
        We may have created temporary files in the job dir, ensure that they disappear.
        """
        if os.path.exists(self.base_config['job_dir']):
            shutil.rmtree(self.base_config['job_dir'])
        if os.path.exists(self.base_config['read_cache']):
            shutil.rmtree(self.base_config['read_cache'])

    def test_wait_until(self):
        """
        Test that the wait function waits until the specified delay after it is first called.
        """
        now = datetime.datetime.now()

        e = executor.Executor(self.base_config)

        e.wait_until(-5)
        self.assertEqual((datetime.datetime.now() - now).seconds, 0)
        e.wait_until(1)
        self.assertEqual((datetime.datetime.now() - now).seconds, 1)
        e.wait_until(3)
        self.assertEqual((datetime.datetime.now() - now).seconds, 3)
        e.wait_until(3)
        self.assertEqual((datetime.datetime.now() - now).seconds, 3)

    def test_job_iterator_config(self):

        config = self.base_config
        config['jobs'] = [ {'name': 'a'}, {'name': 'b'}, {'name': 'c'} ]

        e = executor.Executor(config)

        jobs = e.job_iterator()
        self.assertIsInstance(jobs, types.GeneratorType)
        jobs = list(jobs)

        self.assertEqual(jobs, config['jobs'])

    def test_job_iterator_invalid(self):
        """
        What happens if we pass simply invalid JSON to the job iterator...
        """
        # config['jobs'] expects a list, not a dict
        config = self.base_config
        config['jobs'] = { "test": "oops" }

        e = executor.Executor(config)

        jobs = e.job_iterator()
        self.assertIsInstance(jobs, types.GeneratorType)
        self.assertRaises(AssertionError, lambda: list(jobs))

        # Similarly, it expects a list of objects, not anything else
        config = self.base_config
        config['jobs'] = [1, 2, 3]

        shutil.rmtree(self.base_config['job_dir'])
        e = executor.Executor(config)

        jobs = e.job_iterator()
        self.assertIsInstance(jobs, types.GeneratorType)
        self.assertRaises(AssertionError, lambda: list(jobs))

    def test_job_iterator_files(self):

        input_dir = scratch_tmpdir()
        jobs = [ {'name': 'a'}, {'name': 'b'}, {'name': 'c'} ]
        job_format = os.path.join(input_dir, "wiggle-{}")

        try:
            os.makedirs(input_dir)
            for i, job in enumerate(jobs):
                with open(os.path.join(input_dir, job_format.format(i)), 'w') as f:
                    json.dump(job, f)

            # Without specifying n_jobs, the executor should consume all available jobs

            config = self.base_config
            config['job_format'] = job_format
            e = executor.Executor(config, global_config)

            jobs_out = e.job_iterator()
            self.assertIsInstance(jobs_out, types.GeneratorType)
            jobs_out = list(jobs_out)

            self.assertEqual(jobs_out, jobs)

            # Don't use all of the jobs.

            config['n_jobs'] = 2
            shutil.rmtree(self.base_config['job_dir'])
            e = executor.Executor(config, global_config)

            jobs_out = e.job_iterator()
            self.assertIsInstance(jobs_out, types.GeneratorType)
            jobs_out = list(jobs_out)

            self.assertEqual(len(jobs_out), 2)
            self.assertEqual(jobs_out, jobs[0:2])

            # If we specify more jobs than exist, we should get an error

            config['n_jobs'] = 4
            shutil.rmtree(self.base_config['job_dir'])
            e = executor.Executor(config, global_config)

            jobs_out = e.job_iterator()
            self.assertIsInstance(jobs_out, types.GeneratorType)
            self.assertRaises(AssertionError, lambda: list(jobs_out))

        finally:
            # Ensure this is properly cleaned up, whatever happens
            shutil.rmtree(input_dir)

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
            'read_cache': self.base_config['read_cache']
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
            e = executor.Executor(config_minimal, global_config)

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
                'read_cache': self.base_config['read_cache']
            }

            # And initialise an executor. See what happens!
            e = executor.Executor(config_minimal, global_config)

            self.assertEqual(e.job_format, "job-{}.json")
            self.assertIsNone(e.njobs)
            self.assertIsNone(e.jobs)

            expected_mod = "job_classes/trivial_job.py"
            self.assertEqual(e.job_class_module_file[-len(expected_mod):], expected_mod)

            self.assertIsInstance(e.job_class_module, types.ModuleType)
            self.assertTrue(issubclass(e.job_class, base_job.BaseJob))

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
                'job_format': 'testing-{}',
                'n_jobs': 9999,
                'jobs': ["job1", "job2"],
                'job_class': 'remote_pbs',
                'job_dir': tmpdir2,
                'coordinator_binary': 'a-binary',
                'enable_ipm': True
            }

            # And initialise an executor. See what happens!
            e = executor.Executor(config_minimal, global_config)

            self.assertEqual(e.job_format, "testing-{}")
            self.assertEqual(e.njobs, 9999)
            self.assertEqual(e.jobs, ['job1', 'job2'])

            expected_mod = "job_classes/remote_pbs.py"
            self.assertEqual(e.job_class_module_file[-len(expected_mod):], expected_mod)

            self.assertIsInstance(e.job_class_module, types.ModuleType)
            self.assertTrue(issubclass(e.job_class, job_classes.base_job.BaseJob))

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
