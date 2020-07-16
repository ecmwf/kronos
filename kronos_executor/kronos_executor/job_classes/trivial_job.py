import subprocess

from kronos_executor.synapp_job import SyntheticAppJob

# Very basic job class for running Kronos on a workstation without job
# scheduling system. The job will run on a subprocess.


class Job(SyntheticAppJob):

    def customised_generated_internals(self, script_format):
        super(Job, self).customised_generated_internals(script_format)
        assert script_format['num_nodes'] == 1
