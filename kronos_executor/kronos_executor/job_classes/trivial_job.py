import subprocess

from kronos_executor.synapp_job import SyntheticAppJob

# Very basic job class for running Kronos on a workstation without job
# scheduling system. The job will run on a subprocess.


class Job(SyntheticAppJob):

    allinea_launcher_command = "map --profile mpirun"

    def customised_generated_internals(self, script_format):
        super(Job, self).customised_generated_internals(script_format)
        assert script_format['num_nodes'] == 1

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
