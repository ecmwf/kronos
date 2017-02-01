#!/usr/bin/env python

import datetime
import imp
import json
import os
import sys
import time
import errno

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kronos_executor.global_config import global_config
from kronos_executor import generate_read_files
from kronos_executor.subprocess_callback import SubprocessManager


class Executor(object):
    """
    An Executor passes a schedule of jobs to the real scheduler to be executed.

    Certain elements of the Executor can be overridden by the user.
    """
    class InvalidParameter(Exception):
        pass

    available_parameters = [
        'coordinator_binary', 'enable_ipm', 'job_class', 'job_dir', 'job_format', 'jobs', 'n_jobs', 'ksf_file',
        'procs_per_node', 'read_cache', 'allinea_path', 'allinea_ld_library_path', 'allinea_licence_file',
        'local_tmpdir', 'submission_workers']

    def __init__(self, config, schedule):
        """
        Initialisation. Passed a dictionary of configurations
        """
        # Test for invalid parameters:
        for k in config:
            if k not in self.available_parameters:
                raise self.InvalidParameter("Unknown parameter ({}) supplied".format(k))

        print "Config: {}".format(config)
        self.config = global_config.copy()

        self.schedule = schedule

        self.job_class_module_file = os.path.join(
            os.path.dirname(__file__),
            "job_classes/{}.py".format(config.get("job_class", "trivial_job"))
        )
        print "Job class module: {}".format(self.job_class_module_file)
        self.job_class_module = imp.load_source('job', self.job_class_module_file)
        self.job_class = self.job_class_module.Job

        self.local_tmpdir = config.get("local_tmpdir", None)
        self.job_dir = config.get("job_dir", os.path.join(os.getcwd(), "run"))
        print "Job executing dir: {}".format(self.job_dir)
        if os.path.exists(self.job_dir):
            raise IOError("Path {} already exists".format(self.job_dir))
        os.makedirs(self.job_dir)

        # The binary to use can be overridden in the config file
        try:
            self.coordinator_binary = config['coordinator_binary']
        except KeyError:
            raise KeyError("Coordinator binary not provided in executor configuration")

        self.procs_per_node = config['procs_per_node']

        self.initial_time = None

        # Do we want to use IPM monitoring?
        self.enable_ipm = config.get('enable_ipm', False)
        self.allinea_path = config.get('allinea_path', None)
        self.allinea_licence_file = config.get('allinea_licence_file', None)
        self.allinea_ld_library_path = config.get('allinea_ld_library_path', None)

        self.read_cache_path = config.get("read_cache", None)
        if self.read_cache_path is None:
            raise KeyError("read_cache not provided in schedule config")

        self.thread_manager = SubprocessManager(config.get('submission_workers', 5))

    def run(self):

        # Test the read cache
        print "Testing read cache ..."
        if not generate_read_files.test_read_cache(self.read_cache_path):
            print "Read cache not filled, generating ..."
            generate_read_files.generate_read_cache(self.read_cache_path)
            print "Generated."
        else:
            print "OK."

        # And execute the jobs

        for job_num, job_config in enumerate(self.job_iterator()):
            job_dir = os.path.join(self.job_dir, "job-{}".format(job_num))
            job_config['job_num'] = job_num

            j = self.job_class(job_config, self, job_dir)

            j.generate()
            j.run()

        self.thread_manager.wait()

    def wait_until(self, seconds):
        """
        Wait until n seconds after the first time this routine was called.
        """
        if self.initial_time is None:
            self.initial_time = datetime.datetime.now()

        diff = datetime.datetime.now() - self.initial_time
        diff = 86400 * diff.days + diff.seconds

        if seconds > diff:
            time.sleep(seconds - diff)

    def job_iterator(self):
        """
        Jobs may be specified either a) in the main json file, or b) as a series of json files matching
        the job_format specifier. Abstract dealing with this into another function...

        :return: Returns a generator yielding dictionaries (processed either from the global config, or
                 from the parsed JSONs).
        """

        jobs = self.schedule['jobs']
        assert isinstance(jobs, list)

        for job in jobs:
            assert isinstance(job, dict)
            job_repeats = job.get("repeat", 1)
            for i in range(job_repeats):
                yield job

