import math
import os
import stat
import subprocess

from kronos.executor.job_classes.base_job import BaseJob

job_template = """
#!/bin/sh

module load openmpi

export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"

{profiling_code}

{launcher_command} -np {num_procs} {coordinator_binary} {input_file}

# explicitely call the job terminator signal
python /perm/ma/maab/kronos_tcpip_runs/client_uniq.py "synthetic-app" "0" "complete" {job_num}

"""

allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""



class Job(BaseJob):

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)

        print "Trivial JOBS..."
        print "Inside class {}".format(job_config)
        print "Job dir: {}".format(path)

        self.run_script = os.path.join(self.path, "run_script")
        self.allinea_launcher_command = "map --profile mpirun"
        self.allinea_template = allinea_template

    def generate_internal(self):

        nprocs = self.job_config.get('num_procs', 1)
        nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))
        assert nnodes == 1

        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'coordinator_binary': self.executor.coordinator_binary,
            'num_procs': nprocs,
            'launcher_command': "mpirun",
            'input_file': self.input_file,
            'profiling_code': "",
            'job_num': self.id
        }
               
        if self.executor.allinea_path is not None and self.executor.allinea_ld_library_path is not None:
            script_format['allinea_path'] = self.executor.allinea_path
            script_format['allinea_ld_library_path'] = self.executor.allinea_ld_library_path
            script_format['launcher_command'] = self.allinea_launcher_command
            script_format['profiling_code'] += self.allinea_template.format(**script_format)                

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
        with open('output', 'w') as fout, open('error', 'w') as ferror :
            subprocess.call(self.run_script, stdout=fout, stderr=ferror, stdin=None, shell=True)
        os.chdir(cwd)

        self.executor.set_job_submitted(self.id, [])
