import math
import os
import stat
import subprocess

from kronos.executor.job_classes.base_job import BaseJob

job_template = """
#!/bin/sh

module load openmpi

#export KRONOS_WRITE_DIR="{write_dir}"
#export KRONOS_READ_DIR="{read_dir}"
#export KRONOS_SHARED_DIR="{shared_dir}"

cd {write_dir}

mpirun -np {num_procs} {exe_path} -i {n_files} -k -m
"""


class Job(BaseJob):

    # parameters coming from the KSF job entry
    needed_config_params = [
        "n_files"
    ]

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)

        print "External IOR JOB..."
        print "Inside class {}".format(job_config)
        print "Job dir: {}".format(path)

        # check job configuration
        self.check_job_config()

        self.run_script = os.path.join(self.path, "run_script")

    def check_job_config(self):
        """
        Make sure that all the parameters needed are in the config list
        :return:
        """

        for config_param in self.needed_config_params:
            assert config_param in self.job_config["config_params"]

    def generate_internal(self):

        # Select number of processes and nodes
        nprocs = self.job_config.get('num_procs', 1)
        nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))
        assert nnodes == 1

        # update the template with the config parameters
        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'num_procs': nprocs,
            'input_file': self.input_file,
            "exe_path": "/home/ma/maab/workspace/downloaded_software/IOR/src/C/IOR"
        }

        # update the job submit template with all the configs
        for param_name in self.needed_config_params:
            script_format.update({param_name: self.job_config["config_params"][param_name]})

        with open(self.run_script, 'w') as f:
            f.write(job_template.format(**script_format))

        os.chmod(self.run_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def run(self, depend_jobs_ids):
        """
        'Run' this job

        depend_jobs_ids: The job ids of jobs this depends on. Ignored for trivial executor.

        This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """
        print "Running {}".format(self.run_script)

        cwd = os.getcwd()
        os.chdir(self.path)
        with open('output', 'w') as fout, open('error', 'w') as ferror:
            subprocess.call(self.run_script, stdout=fout, stderr=ferror, stdin=None, shell=True)
        os.chdir(cwd)

        self.executor.set_job_submitted(self.id, [])


