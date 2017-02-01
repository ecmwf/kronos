#!/usr/bin/env python

import datetime
import imp
import json
import os
import sys
import time
import errno

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

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

    def __init__(self, config, global_config, schedule):
        """
        Initialisation. Passed a dictionary of configurations
        """
        # Test for invalid parameters:
        for k in config:
            if k not in self.available_parameters:
                raise self.InvalidParameter("Unknown parameter ({}) supplied".format(k))

        print "Config: {}".format(config)
        self.global_config = global_config

        self.job_format = config.get('job_format', 'job-{}.json')
        self.njobs = config.get('n_jobs', None)
        self.jobs = config.get('jobs', None)
        self.ksf_file = config.get('ksf_file', None)

        if self.njobs is not None:
            print "Num jobs: {}".format(self.njobs)

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
        self.coordinator_binary = config.get('coordinator_binary', self.global_config['coordinator_binary'])

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

        if self.jobs is not None:
            assert isinstance(self.jobs, list)
            for job in self.jobs:
                assert isinstance(job, dict)
                job_repeats = job.get("repeat", 1)
                for i in range(job_repeats):
                    yield job

        elif self.ksf_file is not None:
            print "Reading jobs from ksf file {}".format(self.ksf_file)
            with open(self.ksf_file, 'r') as f:
                ksf_file_fields = json.load(f)

            ksf_jobs = ksf_file_fields['jobs']
            for job in ksf_jobs:
                assert isinstance(job, dict)
                job_repeats = job.get("repeat", 1)
                for i in range(job_repeats):
                    yield job

        else:
            print "Using job specification JSONs, with the format {}".format(self.job_format)

            job_num = 0
            while self.njobs is None or job_num < self.njobs:

                job_filename = self.job_format.format(job_num)

                try:
                    with open(job_filename, 'r') as f:
                        job_config = json.load(f)
                        assert isinstance(job_config, dict)
                        job_repeats = job_config.get("repeat", 1)
                        for i in range(job_repeats):
                            yield job_config

                except IOError as e:
                    assert e.errno == errno.ENOENT
                    assert self.njobs is None
                    return

                job_num += 1

