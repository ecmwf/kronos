import json
import os

from kronos_executor.kronos_events import EventFactory


class BaseJob(object):

    needs_read_cache = True

    def __init__(self, job_config, executor, path):
        self.job_config = job_config
        self.path = path
        self.executor = executor
        self.input_file = os.path.join(self.path, 'input.json')
        self._job_num = job_config.get('job_num', 0)

        self.start_delay = job_config.get('start_delay', 0)
        if not (isinstance(self.start_delay, int) or isinstance(self.start_delay, float)):
            raise TypeError("Start delay must be a number")

        self.depends = self.build_dependencies()

        # True if this job is accounted for simulation runtime
        self.is_job_timed = job_config.get('timed', False)

    @property
    def id(self):
        return self._job_num

    def build_dependencies(self):
        """
        Build the list of dependencies for this job
        :return:
        """

        # -------------------------------------------------------------------------
        # NOTE: dependencies can be either:
        #
        #  - integers (i.e. as used by the executor_schedule and interpreted
        #    as job-id to be completed)
        #
        #  - dictionaries (i.e. as used by the executor_events and used to describe
        #    event-based dependencies)
        # -------------------------------------------------------------------------

        _deps = []
        if self.job_config.get('depends', []):

            if all(isinstance(d, int) for d in self.job_config['depends']):
                _deps = self.job_config.get('depends', [])

            elif all(isinstance(d, dict) for d in self.job_config['depends']):
                _deps = [EventFactory.from_dictionary(d, validate_event=False) for d in self.job_config['depends']]
            else:
                raise ValueError(("Dependencies for job {} must be either " +
                                  "all integers or all 'kronos-event' dictionaries").format(self._job_num))

        return _deps

    def generate(self):

        # Ensure that the directory the job is to run in exists
        if os.path.exists(self.path):
            raise IOError("Path already exists: {}".format(self.path))
        os.mkdir(self.path)

        self.generate_internal()

        # Put the json (to be passed to the synthetic app) inside the directory. This runs after
        # generate so that (in principle) the derived class can modify the input during the
        # generate_internal call.
        with open(self.input_file, 'w') as f:
            json.dump(self.job_config, f)

    def generate_internal(self):
        raise NotImplementedError

    def run(self, depend_job_ids):
        """
        'Run' this job

        depend_jobs_ids: The job ids of jobs this depends on.

        n.b. This class is responsible for calling set_job_submitted on the kronos_executor.

        This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """
        raise NotImplementedError
