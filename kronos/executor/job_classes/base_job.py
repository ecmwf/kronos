import json
import os


class BaseJob(object):

    def __init__(self, job_config, executor, path):
        self.job_config = job_config
        self.path = path
        self.executor = executor
        self.input_file = os.path.join(self.path, 'input.json')
        self.jobno = job_config.get('job_num', 0)

        self.start_delay = job_config.get('start_delay', 0)
        if not (isinstance(self.start_delay, int) or isinstance(self.start_delay, float)):
            raise TypeError("Start delay must be a number")

    def generate(self):

        # Ensure that the directory the job is to run in exists
        if os.path.exists(self.path):
            raise IOError("Path already exists: {}".format(self.path))
        os.mkdir(self.path)

        self.generate_internal()

        # Put the json (to be passed to the synthetic app) inside the directory. This runs after generate so that
        # (in principle) the derived class can modify the input during the generate_internal call.
        with open(self.input_file, 'w') as f:
            json.dump(self.job_config, f)

    def generate_internal(self):
        raise NotImplementedError

    def run(self):
        """
        'Run' this job

        This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """
        raise NotImplementedError
