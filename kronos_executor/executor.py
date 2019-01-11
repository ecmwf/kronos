#!/usr/bin/env python

import datetime
import imp
import logging
import os
import socket
import sys
import time
import uuid
from shutil import copy2

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kronos_executor import global_config
from kronos_executor import generate_read_files
from kronos_executor.subprocess_callback import SubprocessManager


# -------- setup a logger for the kronos_executor --------
logger = logging.getLogger("kronos.kronos_executor")
logger.setLevel(logging.INFO)

msg_format = '%(asctime)s; %(name)s; %(levelname)s; %(message)s'
# msg_format = '%(created)f %(message)s'

# to file
fh = logging.FileHandler('kronos-kronos_executor.log', mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(msg_format))
logger.addHandler(fh)

# to stdout
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(msg_format))
logger.addHandler(ch)


class Executor(object):
    """
    An Executor passes a schedule of jobs to the real scheduler to be executed.

    Certain elements of the Executor can be overridden by the user.
    """
    class InvalidParameter(Exception):
        pass

    available_parameters = [
        'coordinator_binary',
        'enable_ipm',
        'job_class',
        'job_dir',
        'job_dir_shared',
        'procs_per_node',
        'read_cache',
        'allinea_path',
        'allinea_ld_library_path',
        'allinea_licence_file',
        'local_tmpdir',
        'submission_workers',
        'enable_darshan',
        'darshan_lib_path',
        'file_read_multiplicity',
        'file_read_size_min_pow',
        'file_read_size_max_pow',

        # options specific for event based submission
        'execution_mode',
        'notification_host',
        'notification_port',
        'time_event_cycles',
        'event_batch_size',
        'n_submitters'

    ]

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """
        # Test for invalid parameters:
        for k in config:
            if k not in self.available_parameters:
                raise self.InvalidParameter("Unknown parameter ({}) supplied".format(k))

        self.config = global_config.copy()
        self.config.update(config)

        # A token that uniquely identifies the simulation
        self.simulation_token = uuid.uuid4()

        logger.info("Config: {}".format(config))
        logger.info("Simulation Token: {}".format(self.simulation_token))

        self.schedule = schedule

        # job dir
        self.local_tmpdir = config.get("local_tmpdir", None)
        self.job_dir = config.get("job_dir", os.path.join(os.getcwd(), "run"))

        # its own dir
        self.executor_file_dir = os.path.dirname(__file__)

        logger.info("Job executing dir: {}".format(self.job_dir))

        # take the timestamp to be used to archive run folders (if existing)
        time_stamp_now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        if os.path.exists(self.job_dir):
            time_stamped_output = self.job_dir+"."+time_stamp_now
            logger.warning("Path {} already exists, moving it into: {}".format(self.job_dir, time_stamped_output))
            os.rename(self.job_dir, time_stamped_output)

        os.makedirs(self.job_dir)

        if kschedule_file:
            copy2(kschedule_file, self.job_dir)

        # shared dir
        self.job_dir_shared = config.get("job_dir_shared", os.path.join(os.getcwd(), "run/shared"))
        logger.info("Shared output directory: {}".format(self.job_dir_shared))

        if os.path.exists(self.job_dir_shared):
            time_stamped_shared = self.job_dir_shared + "." + time_stamp_now
            logger.warning("Path {} already exists, moving it into: {}".format(self.job_dir_shared, time_stamped_shared))
            os.rename(self.job_dir_shared, time_stamped_shared)

        os.makedirs(self.job_dir_shared)

        # The binary to use can be overridden in the config file
        try:
            self.coordinator_binary = config['coordinator_binary']
        except KeyError:
            raise KeyError("Coordinator binary not provided in kronos_executor configuration")

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

        if self._file_read_multiplicity or self._file_read_size_min_pow or self._file_read_size_max_pow:
            logger.info("Using customised read cache parameters: ")
            logger.info("Read cache multiplicity: {}".format(self._file_read_multiplicity))
            logger.info("File read min size (2 ^ {}) bytes".format(self._file_read_size_min_pow))
            logger.info("File read max size (2 ^ {}) bytes".format(self._file_read_size_max_pow))

        # check the EVENTS execution mode settings
        self.execution_mode = config.get('execution_mode', "events")

        if config.get('execution_mode') == "events" and config.get('submission_workers'):
            raise KeyError("parameter 'submission_workers' should only be set if execution_mode = scheduler")

        if config.get('execution_mode') != "events" and config.get('notification_host'):
            raise KeyError("parameter 'notification_host' should only be set if execution_mode = events")
        else:
            self.notification_host = config.get('notification_host', socket.gethostname())

        if config.get('execution_mode') != "events" and config.get('notification_port'):
            raise KeyError("parameter 'notification_port' should only be set if execution_mode = events")
        else:
            self.notification_port = config.get('notification_port', 7363)

        if config.get('execution_mode') != "events" and config.get('time_event_cycles'):
            raise KeyError("parameter 'time_event_cycles' should only be set if execution_mode = events")
        else:
            self.time_event_cycles = config.get('time_event_cycles', 1)

        if config.get('execution_mode') != "events" and config.get('event_batch_size'):
            raise KeyError("parameter 'event_batch_size' should only be set if execution_mode = events")
        else:
            self.time_event_cycles = config.get('event_batch_size', 1)

        if config.get('execution_mode') != "events" and config.get('n_submitters'):
            raise KeyError("parameter 'n_submitters' should only be set if execution_mode = events")
        else:
            self.time_event_cycles = config.get('n_submitters', 1)

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

    def generate_job_internals(self):

        # Test the read cache
        logger.info("Testing read cache ...")
        if not generate_read_files.test_read_cache(
                self.read_cache_path,
                self._file_read_multiplicity,
                self._file_read_size_min_pow,
                self._file_read_size_max_pow):

            logger.info("Read cache not filled, generating ...")

            generate_read_files.generate_read_cache(
                self.read_cache_path,
                self._file_read_multiplicity,
                self._file_read_size_min_pow,
                self._file_read_size_max_pow
            )
            logger.info("Generated.")
        else:
            logger.info("OK.")

        # Launched jobs, matched with their job-ids

        jobs = []

        for job_num, job_config in enumerate(self.job_iterator()):
            job_dir = os.path.join(self.job_dir, "job-{}".format(job_num))
            job_config['job_num'] = job_num

            # get job template name (either from the job config or from the global config, if present)
            job_template_name = job_config.get("job_class", self.config.get("job_class", "trivial_job"))
            job_classes_dir = os.path.join(os.path.dirname(__file__), "job_classes")

            # now search for the template file in the cwd first..
            if os.path.isfile( "{}.py".format(os.path.join(os.getcwd(), job_template_name)) ):

                job_class_module_file = "{}.py".format(os.path.join(os.getcwd(), job_template_name))

            elif os.path.isfile( os.path.join(job_classes_dir, "{}.py".format(job_template_name)) ):

                job_class_module_file = os.path.join(os.path.dirname(__file__), "job_classes/{}.py".format(job_template_name))

            else:

                logger.error("template file {}.py not found neither in {} nor in {}".format(job_template_name,
                                                                                            os.getcwd(),
                                                                                            job_classes_dir))
                raise IOError

            # now load the module
            job_class_module = imp.load_source('job', job_class_module_file)

            job_class = job_class_module.Job

            if self._file_read_multiplicity:
                job_config['file_read_multiplicity'] = self._file_read_multiplicity

            if self._file_read_size_min_pow:
                job_config['file_read_size_min_pow'] = self._file_read_size_min_pow

            if self._file_read_size_max_pow:
                job_config['file_read_size_max_pow'] = self._file_read_size_max_pow

            if self.execution_mode == "events":
                job_config['notification_host'] = self.notification_host
                job_config['notification_port'] = self.notification_port

            j = job_class(job_config, self, job_dir)

            j.generate()
            jobs.append(j)

        return jobs

    def run(self):
        """
        Main function that manages the execution of the schedule
        :return:
        """

        raise NotImplementedError

    def unsetup(self):
        """
        Does any post-run tasks (e.g. clean-up files, etc..)
        :return:
        """

        print "all done."
