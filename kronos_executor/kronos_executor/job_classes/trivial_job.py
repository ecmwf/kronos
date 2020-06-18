import math
import os
import stat
import subprocess

from kronos_executor.synapp_job import SyntheticAppJob

# Very basic job submit template for running Kronos on a workstation
# without job scheduling system. The job will run on a subprocess.


job_template = """
#!/bin/sh

module load openmpi

export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"
export KRONOS_TOKEN="{simulation_token}"

{profiling_code}

cd {write_dir}

{launcher_command} -np {num_procs} {coordinator_binary} {input_file}

"""

allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""


class Job(SyntheticAppJob):

    submit_script_template = job_template
    allinea_template = allinea_template
    launcher_command = "mpirun"
    allinea_launcher_command = "map --profile mpirun"

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)

    def customised_generated_internals(self, script_format):
        super(Job, self).customised_generated_internals(script_format)
        assert script_format['num_nodes'] == 1

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
