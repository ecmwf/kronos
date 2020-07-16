#!/usr/bin/env python

import datetime
import imp
import logging
import os
import socket
import stat
import sys
import time
import uuid
from shutil import copy2
import subprocess

from kronos_executor import log_msg_format
from kronos_executor.global_config import global_config
from kronos_executor import generate_read_files
from kronos_executor.execution_context import load_context
from kronos_executor.job_submitter import JobSubmitter

logger = logging.getLogger(__name__)


class Executor(object):
    """
    An Executor passes a time_schedule of jobs to the real scheduler to be executed.

    Certain elements of the Executor can be overridden by the user.
    """
    class InvalidParameter(Exception):
        pass

    available_parameters = [
        'coordinator_binary',
        'execution_context',
        'job_class',
        'job_template',
        'job_dir',
        'job_dir_shared',
        'procs_per_node',
        'read_cache',
        'local_tmpdir',
        'n_submitters',
        'file_read_multiplicity',
        'file_read_size_min_pow',
        'file_read_size_max_pow',

        # ioserver options
        'ioserver_hosts_file',

        # options specific for event based submission
        'execution_mode',
        'notification_host',
        'notification_port',
        'time_event_cycles',
        'event_batch_size',

        # if NVRAM is to be used
        'nvdimm_root_path'

    ]

    def __init__(self,
                 config,
                 schedule,
                 arg_config=None
                 ):
        """
        Initialisation. Passed a dictionary of configurations
        """

        # root logs directed to file only if the executor is instantiated..
        self.logfile_path = os.path.join(os.getcwd(), "kronos-executor.log")
        root_logger = logging.getLogger()
        fh = logging.FileHandler(self.logfile_path, mode='w')
        fh.setFormatter(logging.Formatter(log_msg_format))
        fh.setLevel(logging.DEBUG)
        root_logger.addHandler(fh)

        # Test for invalid parameters:
        for k in config:
            if k not in self.available_parameters:
                raise self.InvalidParameter("Unknown parameter ({}) supplied".format(k))

        self.config = global_config.copy()
        self.config.update(config)

        # config options passed from command line
        self.arg_config = arg_config if arg_config else {}

        # list of jobs will be created in the setup phase
        self.jobs = None

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
            self.job_dir = self.job_dir.rstrip('/')
            time_stamped_output = self.job_dir+"."+time_stamp_now
            logger.warning("Path {} already exists, moving it into: {}".format(self.job_dir, time_stamped_output))
            os.rename(self.job_dir, time_stamped_output)

        os.makedirs(self.job_dir)

        kschedule_file = self.arg_config.get("kschedule_file")
        if kschedule_file:
            copy2(kschedule_file, self.job_dir)

        # shared dir
        self.job_dir_shared = config.get("job_dir_shared", os.path.join(os.getcwd(), "run/shared"))
        logger.info("Shared output directory: {}".format(self.job_dir_shared))

        if os.path.exists(self.job_dir_shared):
            self.job_dir_shared = self.job_dir_shared.rstrip('/')
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

        search_paths = [
            os.getcwd(),
            os.path.join(os.path.dirname(__file__), "execution_contexts")
        ]
        self.execution_context = load_context(config.get('execution_context', 'trivial'), search_paths, config)

        self.cancel_file_path = os.path.join(self.job_dir, "killjobs")
        self.cancel_file = None

        self.read_cache_path = config.get("read_cache", None)
        if self.read_cache_path is None:
            raise KeyError("read_cache not provided in time_schedule config")

        self.job_submitter = JobSubmitter(config.get('n_submitters', 4))

        self.submitted_job_ids = {}

        # n.b.
        self._file_read_multiplicity = config.get('file_read_multiplicity', None)
        self._file_read_size_min_pow = config.get('file_read_size_min_pow', None)
        self._file_read_size_max_pow = config.get('file_read_size_max_pow', None)

        if self._file_read_multiplicity or self._file_read_size_min_pow or self._file_read_size_max_pow:
            logger.info("Using customised read cache parameters: ")
            logger.info("Read cache multiplicity: {}".format(self._file_read_multiplicity))
            logger.info("File read min size (2 ^ {}) bytes".format(self._file_read_size_min_pow))
            logger.info("File read max size (2 ^ {}) bytes".format(self._file_read_size_max_pow))

        # path to the ioserver hosts.json file
        self.ioserver_hosts_file = config.get('ioserver_hosts_file', None)

        # check the EVENTS execution mode settings
        self.execution_mode = config.get('execution_mode', "events")

        if self.execution_mode != "events" and config.get('notification_host'):
            raise KeyError("parameter 'notification_host' should only be set if execution_mode = events")
        else:
            self.notification_host = config.get('notification_host', socket.gethostname())

        if self.execution_mode != "events" and config.get('notification_port'):
            raise KeyError("parameter 'notification_port' should only be set if execution_mode = events")
        else:
            self.notification_port = config.get('notification_port', 7363)

        if self.execution_mode != "events" and config.get('time_event_cycles'):
            raise KeyError("parameter 'time_event_cycles' should only be set if execution_mode = events")
        else:
            self.time_event_cycles = config.get('time_event_cycles', 1)

        if self.execution_mode != "events" and config.get('event_batch_size'):
            raise KeyError("parameter 'event_batch_size' should only be set if execution_mode = events")

        # nvdimm path if present
        self.nvdimm_root_path = self.config.get("nvdimm_root_path")

    def set_job_submitted(self, job_num, submitted_id):
        self.submitted_job_ids[job_num] = submitted_id

        if submitted_id:
            first = self.cancel_file is None
            if first:
                self.cancel_file = open(self.cancel_file_path, 'w')
                os.chmod(self.cancel_file_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

            self.cancel_file.write(self.execution_context.cancel_entry(submitted_id, first))
            self.cancel_file.flush()

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

        # Launched jobs, matched with their job-ids
        jobs = []
        need_cache = False

        for job_num, job_config in enumerate(self.job_iterator()):
            job_dir = os.path.join(self.job_dir, "job-{}".format(job_num))
            job_config['job_num'] = job_num

            # get job template name (either from the job config or from the global config, if present)
            job_class_name = job_config.setdefault("job_class", self.config.get("job_class", "synapp"))
            job_classes_dir = os.path.join(os.path.dirname(__file__), "job_classes")

            # now search for the template file in the cwd first..
            if os.path.isfile( "{}.py".format(os.path.join(os.getcwd(), job_class_name)) ):

                job_class_module_file = "{}.py".format(os.path.join(os.getcwd(), job_class_name))

            elif os.path.isfile( os.path.join(job_classes_dir, "{}.py".format(job_class_name)) ):

                job_class_module_file = os.path.join(os.path.dirname(__file__), "job_classes/{}.py".format(job_class_name))

            else:

                logger.error("template file {}.py not found neither in {} nor in {}".format(job_class_name,
                                                                                            os.getcwd(),
                                                                                            job_classes_dir))
                raise IOError

            # now load the module
            job_module_name = "kronos_job_class_{}".format(job_class_name)
            if job_module_name in sys.modules:
                job_class_module = sys.modules[job_module_name]
            else:
                job_class_module = imp.load_source(job_module_name, job_class_module_file)

            job_class = job_class_module.Job
            need_cache = need_cache or job_class.needs_read_cache

            # Enrich the job configuration with the necessary global configurations
            # coming from the configuration file
            if self._file_read_multiplicity:
                job_config['file_read_multiplicity'] = self._file_read_multiplicity

            if self._file_read_size_min_pow:
                job_config['file_read_size_min_pow'] = self._file_read_size_min_pow

            if self._file_read_size_max_pow:
                job_config['file_read_size_max_pow'] = self._file_read_size_max_pow

            if self.execution_mode == "events":
                job_config['notification_host'] = self.notification_host
                job_config['notification_port'] = self.notification_port

            if self.config.get("nvdimm_root_path"):
                job_config['nvdimm_root_path'] = self.nvdimm_root_path

            j = job_class(job_config, self, job_dir)

            j.generate()
            jobs.append(j)

        # Test the read cache if needed
        if need_cache:
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
        else:
            logger.info("Not testing read cache, no job needs it.")

        return jobs

    def setup(self):
        """
        General placeholder for setting up the simulation
        Each executor might have different things to setup.
        :return:
        """

        logger.info("Executing setup..")

        self.jobs = self.generate_job_internals()

        logger.info("Setup executed")

    def epilogue(self):
        """
        Runs the epilogue as defined in the kschedule
        :return:
        """

        epilogue = self.schedule.epilogue
        logger.info("Executing epilogue..")
        logger.debug("epilogue: {}".format(epilogue))

        if not epilogue:
            return

        # Executes all the scripts in sequence
        for tt, task in enumerate(epilogue.get("tasks")):

            script_abs_path = os.path.join(os.getcwd(), task["script"])
            logger.info("Executing script: {}".format(script_abs_path))

            if not os.path.isfile(script_abs_path):
                print("Executing epilogue task {}, but script {} not found!".format(tt, script_abs_path))
                raise IOError

            _proc = subprocess.Popen([script_abs_path],
                                     shell=False, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            _proc.wait()

            proc_stdout = _proc.stdout.readlines()
            proc_stderr = _proc.stderr.readlines()

            logger.debug("script stdout: {}".format(proc_stdout))
            logger.debug("script stderr: {}".format(proc_stderr))

    def do_run(self):
        """
        Runs the jobs as defined in the kschedule
        :return:
        """

        raise NotImplementedError

    def __enter__(self):
        self.setup()

    def __exit__(self, exc_type, exc_value, traceback):
        self.teardown(exc_type is not None)

    def run(self):
        """
        Main function that manages the execution of the time_schedule,
        it includes the following phases:

        - setup()
        - prologue()
        - do_run()
        - epilogue()
        - teardown()
        """

        with self:
            # only do the actual run when is not a dry run
            if not self.arg_config.get("dry_run"):

                # executes the prologue of the kschedule
                self.prologue()

                # do the actual run (submits the kschedule jobs)
                self.do_run()

                # executes the epilogue of the kschedule
                self.epilogue()

    def prologue(self):
        """
        Runs the epilogue as defined in the kschedule
        :return:
        """

        prologue = self.schedule.prologue
        logger.info("Executing prologue..")
        logger.debug("prologue: {}".format(prologue))

        if not prologue:
            return

        # Executes all the scripts in sequence
        for tt, task in enumerate(prologue.get("tasks")):

            script_abs_path = os.path.join(os.getcwd(), task["script"])
            logger.info("Executing script: {}".format(script_abs_path))

            if not os.path.isfile(script_abs_path):
                print("Executing prologue task {}, but script {} not found!".format(tt, script_abs_path))
                raise IOError

            _proc = subprocess.Popen([script_abs_path],
                                     shell=False, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            _proc.wait()

            proc_stdout = _proc.stdout.readlines()
            proc_stderr = _proc.stderr.readlines()

            logger.debug("script stdout: {}".format(proc_stdout))
            logger.debug("script stderr: {}".format(proc_stderr))

    def teardown(self, error=False):
        """
        General placeholder for tearing down the simulation
        Each executor might have different things to clean up
        (e.g. clean-up files, etc..).
        :return:
        """

        if self.cancel_file is not None:
            self.cancel_file.close()

        self.job_submitter.close()

        # Copy the log file into the output directory
        if os.path.exists(self.logfile_path):
            if error:
                logger.error("kronos simulation failed.".upper())
            else:
                logger.info("kronos simulation completed.".upper())
            logger.info("copying {} into {}".format(self.logfile_path, self.job_dir))
            copy2(self.logfile_path, self.job_dir)
