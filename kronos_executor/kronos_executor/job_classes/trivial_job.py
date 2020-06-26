import math
import os
import stat
import subprocess

from kronos_executor.synapp_job import SyntheticAppJob

# Very basic job class for running Kronos on a workstation without job
# scheduling system. The job will run on a subprocess.


class Job(SyntheticAppJob):

    launcher_command = "mpirun"
    allinea_launcher_command = "map --profile mpirun"

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)

    def customised_generated_internals(self, script_format):
        super(Job, self).customised_generated_internals(script_format)
        assert script_format['num_nodes'] == 1

        script_format['scheduler_params'] = ""
        script_format['env_setup'] = "module load openmpi"
        script_format['launch_command'] = "{launcher_command} -np {num_procs}".format(**script_format)

    def get_submission_arguments(self, depend_job_ids):
        return ["bash", self.submit_script]

    def run(self, depend_jobs_ids):
        """
        'Run' this job

        depend_jobs_ids: The job ids of jobs this depends on. Ignored for trivial kronos_executor.

        This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """
        print("Running {}".format(self.submit_script))

        with open('output', 'w') as fout, open('error', 'w') as ferror :
            subprocess.call(self.submit_script, stdout=fout, stderr=ferror, stdin=None, shell=True)

        self.executor.set_job_submitted(self.id, [])
