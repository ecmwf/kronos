import os
import stat
import subprocess

from kronos.executor.job_classes.base_job import BaseJob

job_template = """
#!/bin/sh

#module load openmpi

export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"

{profiling_code}

python /perm/ma/maab/kronos_tcpip_runs/send_complete_msg.py nebula 7363 {job_num} > {write_dir}/output_{job_num}.log 2>&1

"""

allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""


class Job(BaseJob):

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)

        # print "Trivial JOBS..."
        # print "Inside class {}".format(job_config)
        # print "Job dir: {}".format(path)

        self.run_script = os.path.join(self.path, "run_script")
        self.allinea_launcher_command = "map --profile mpirun"
        self.allinea_template = allinea_template

    def generate_internal(self):

        nprocs = self.job_config.get('num_procs', 1)
        # nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))
        # # assert nnodes == 1

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

    def submission_callback(self, output):
        """
        This is the callback function
        """

        self.executor.set_job_submitted(self.id, "")

    def get_submission_and_callback_params(self):
        """
        Get all the necessary params to call the static version of the callback
        (needed to use of the static callback function without job instance..)
        :return:
        """

        return {
            "jid": self.id,
            "submission_params": self.get_submission_arguments(),
            "callback_params": None
        }

    @staticmethod
    def submission_callback_static(output, callback_params):
        """
        Static function that does the callback (everything but informing the executor of the
        submission of the jobs! - record of the submitted jobs has to be handled by the caller)
        :param output:
        :param callback_params:
        :return:
        """

        # nothing to do for the trivial job..
        pass

    def get_submission_arguments(self):
        subprocess_args = ["bash", self.run_script]
        subprocess_args = ' '.join(subprocess_args).split(' ')
        return subprocess_args

    def get_killall_script_name(self):
        return None

    def run(self, depend_job_ids, multi_threading=True):
        """
        'Run' this job

            This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """

        self.executor.wait_until(self.start_delay)

        print "Submitting job {}".format(self.id)

        # multi-threaded option
        if multi_threading:

            self.executor.thread_manager.subprocess_callback(self.submission_callback, *self.get_submission_arguments())

        else:  # run in a subprocess and call the callback manually

            output = subprocess.check_output(self.get_submission_arguments())
            self.submission_callback(output)
