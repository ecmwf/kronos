#!/usr/bin/env python

import datetime
import imp
import os
import sys
import time
from shutil import copy2

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kronos.executor.global_config import global_config
from kronos.executor import generate_read_files
from kronos.executor.subprocess_callback import SubprocessManager


class Executor(object):
    """
    An Executor passes a schedule of jobs to the real scheduler to be executed.

    Certain elements of the Executor can be overridden by the user.
    """
    class InvalidParameter(Exception):
        pass

    available_parameters = [
        'coordinator_binary', 'enable_ipm', 'job_class', 'job_dir', 'job_dir_shared',
        'procs_per_node', 'read_cache', 'allinea_path', 'allinea_ld_library_path', 'allinea_licence_file',
        'local_tmpdir', 'submission_workers', 'enable_darshan', 'darshan_lib_path',
        'file_read_multiplicity', 'file_read_min_size_pow', 'file_read_max_size_pow']

    def __init__(self, config, schedule, ksf_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """
        # Test for invalid parameters:
        for k in config:
            if k not in self.available_parameters:
                raise self.InvalidParameter("Unknown parameter ({}) supplied".format(k))

        print "Config: {}".format(config)
        self.config = global_config.copy()
        self.config.update(config)

        self.schedule = schedule

        self.job_class_module_file = os.path.join(
            os.path.dirname(__file__),
            "job_classes/{}.py".format(config.get("job_class", "trivial_job"))
        )
        print "Job class module: {}".format(self.job_class_module_file)
        self.job_class_module = imp.load_source('job', self.job_class_module_file)
        self.job_class = self.job_class_module.Job

        # job dir
        self.local_tmpdir = config.get("local_tmpdir", None)
        self.job_dir = config.get("job_dir", os.path.join(os.getcwd(), "run"))
        print "Job executing dir: {}".format(self.job_dir)
        if os.path.exists(self.job_dir):
            raise IOError("Path {} already exists".format(self.job_dir))
        os.makedirs(self.job_dir)

        if ksf_file:
            copy2(ksf_file, self.job_dir)

        # shared dir
        self.job_dir_shared = config.get("job_dir_shared", os.path.join(os.getcwd(), "run/shared"))
        print "Shared output directory: {}".format(self.job_dir_shared)
        if os.path.exists(self.job_dir_shared):
            raise IOError("Path {} already exists".format(self.job_dir_shared))
        os.makedirs(self.job_dir_shared)

        # The binary to use can be overridden in the config file
        try:
            self.coordinator_binary = config['coordinator_binary']
        except KeyError:
            raise KeyError("Coordinator binary not provided in executor configuration")

        self.procs_per_node = config['procs_per_node']

        self.initial_time = None

        # Do we want to use IPM monitoring?
        self.enable_ipm = config.get('enable_ipm', False)

        # Do we want to use Darshan monitoring?
        self.enable_darshan = config.get('enable_darshan', False)
        self.darshan_lib_path = config.get('darshan_lib_path', None)

        # Do we want to use Allinea monitoring?
        self.allinea_path = config.get('allinea_path', None)
        self.allinea_licence_file = config.get('allinea_licence_file', None)
        self.allinea_ld_library_path = config.get('allinea_ld_library_path', None)

        self.read_cache_path = config.get("read_cache", None)
        if self.read_cache_path is None:
            raise KeyError("read_cache not provided in schedule config")

        self.thread_manager = SubprocessManager(config.get('submission_workers', 5))

        self._submitted_jobs = {}

        # n.b.
        self._file_read_multiplicity = config.get('file_read_multiplicity', None)
        self._file_read_size_min_pow = config.get('file_read_size_min_pow', None)
        self._file_read_size_max_pow = config.get('file_read_size_max_pow', None)

    def run(self):

        # Test the read cache
        print "Testing read cache ..."
        if not generate_read_files.test_read_cache(
                self.read_cache_path,
                self._file_read_multiplicity,
                self._file_read_size_min_pow,
                self._file_read_size_max_pow):

            print "Read cache not filled, generating ..."

            generate_read_files.generate_read_cache(
                self.read_cache_path,
                self._file_read_multiplicity,
                self._file_read_size_min_pow,
                self._file_read_size_max_pow
            )
            print "Generated."
        else:
            print "OK."

        # Launched jobs, matched with their job-ids

        jobs = []

        for job_num, job_config in enumerate(self.job_iterator()):
            job_dir = os.path.join(self.job_dir, "job-{}".format(job_num))
            job_config['job_num'] = job_num

            if self._file_read_multiplicity:
                job_config['file_read_multiplicity'] = self._file_read_multiplicity
            if self._file_read_size_min_pow:
                job_config['file_read_size_min_pow'] = self._file_read_size_min_pow
            if self._file_read_size_max_pow:
                job_config['file_read_size_max_pow'] = self._file_read_size_max_pow

            j = self.job_class(job_config, self, job_dir)

            j.generate()
            jobs.append(j)

        # Work through the list of jobs. Launch the first job that is not blocked by any other
        # job.
        # n.b. job.run does not just simply return the ID, as it may be asynchronous. The job
        # handler is responsible for calling back set_job_submitted

        while len(jobs) != 0:
            nqueueing = self.thread_manager.num_running

            found_job = None
            depend_ids = None
            for j in jobs:

                try:
                    depends = j.depends
                    depend_ids = [self._submitted_jobs[d] for d in depends]

                    # We have found a job. Break out of the search
                    found_job = j
                    break

                except KeyError:
                    # Go on to the next job in the list
                    pass

            if found_job:
                found_job.run(depend_ids)
                jobs.remove(found_job)

            else:
                # If there are no unblocked jobs, but there are still jobs in the submit queue,
                # wait until something happens
                self.thread_manager.wait_until(nqueueing-1)

        # Wait until we are done

        self.thread_manager.wait()

    def set_job_submitted(self, job_num, submitted_id):
        self._submitted_jobs[job_num] = submitted_id

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

        jobs = self.schedule.synapp_data
        assert isinstance(jobs, list)

        for job in jobs:
            assert isinstance(job, dict)
            job_repeats = job.get("repeat", 1)
            for i in range(job_repeats):
                yield job
