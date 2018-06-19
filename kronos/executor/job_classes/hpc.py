import math
import os
import stat
import subprocess

from kronos.executor.job_classes.base_job import BaseJob


class HPCJob(BaseJob):

    submit_script_template = None
    ipm_template = None
    darshan_template = None
    allinea_template = None
    submit_command = None
    depend_parameter = None
    depend_separator = None
    launcher_command = None
    allinea_launcher_command = None

    cancel_file = None
    cancel_file_head = None
    cancel_file_line = None

    def __init__(self, job_config, executor, path):
        super(HPCJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")

    def generate_internal(self):

        nprocs = self.job_config.get('num_procs', 1)
        nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))

        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'procs_per_node': min(self.executor.procs_per_node, nprocs),
            'coordinator_binary': self.executor.coordinator_binary,
            'coordinator_library_path': os.path.join( os.path.dirname(self.executor.coordinator_binary),"../lib"),     
            'queue': 'np',
            'num_procs': nprocs,
            'num_nodes': nnodes,
            'num_hyperthreads': 1,
            'input_file': self.input_file,
            'job_dir': self.path,
            'profiling_code': "",
            'experiment_id': 'synthApp_{}_{}_{}'.format(nprocs, nnodes, self.id),
            'job_num': self.id,
            'job_output_file': os.path.join(self.path, "output"),
            'job_error_file': os.path.join(self.path, "error"),
            'launcher_command': self.launcher_command
        }

        # Enable IPM logging if desired
        if self.executor.enable_ipm:
            script_format['profiling_code'] += self.ipm_template.format(**script_format)

        # Enable Darshan logging if desired
        if self.executor.enable_darshan:
            script_format['darshan_lib_path'] = self.executor.darshan_lib_path
            script_format['profiling_code'] += self.darshan_template.format(**script_format)

        # Enable Allinea map if desired
        if self.executor.allinea_path is not None and self.executor.allinea_ld_library_path is not None:
            script_format['allinea_path'] = self.executor.allinea_path
            script_format['allinea_ld_library_path'] = self.executor.allinea_ld_library_path
            script_format['launcher_command'] = self.allinea_launcher_command
            
            # append allinea licence file if specified..
            if self.executor.allinea_licence_file:
                script_format['allinea_licence_file'] = self.executor.allinea_licence_file
                self.allinea_template += self.allinea_lic_file_template
            script_format['profiling_code'] += self.allinea_template.format(**script_format)

        with open(self.submit_script, 'w') as f:
            f.write(self.submit_script_template.format(**script_format))

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def submission_callback(self, output):
        """
        This is the callback function
        """
        if HPCJob.cancel_file is None:
            cancel_file_path = os.path.join(self.executor.job_dir, "killjobs")
            HPCJob.cancel_file = open(cancel_file_path, 'w')
            HPCJob.cancel_file.write(self.cancel_file_head)
            os.chmod(cancel_file_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

        # Sequence_id_job = filter(str.isdigit, output)
        sequence_id_job = output.strip()
        HPCJob.cancel_file.write(self.cancel_file_line.format(sequence_id=sequence_id_job))
        HPCJob.cancel_file.flush()

        self.executor.set_job_submitted(self.id, sequence_id_job)

    def get_submission_and_callback_params(self, depend_job_ids=None):
        """
        Get all the necessary params to call the static version of the callback
        (needed to use of the static callback function without job instance..)
        :return:
        """

        depend_job_ids = depend_job_ids if depend_job_ids else []

        return {
            "jid": self.id,
            "submission_params": self.get_submission_arguments(depend_job_ids),
            "callback_params": {
                                "cancel_file": HPCJob.cancel_file,
                                "executor_job_dir": self.executor.job_dir,
                                "cancel_file_head": self.cancel_file_head,
                                "cancel_file_line": self.cancel_file_line
                                }
        }

    @staticmethod
    def submission_callback_static(self, output, **callback_params):
        """
        Static function that does the callback (everything but informing the executor of the
        submission of the jobs! - record of the submitted jobs has to be handled by the caller)
        :param self:
        :param output:
        :param callback_params:
        :return:
        """

        if callback_params["callback_params"]["cancel_file"] is None:
            cancel_file_path = os.path.join(callback_params["callback_params"]["executor_job_dir"], "killjobs")
            cancel_file = open(cancel_file_path, 'w')
            cancel_file.write(self.cancel_file_head)
            os.chmod(cancel_file_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

        # Sequence_id_job = filter(str.isdigit, output)
        sequence_id_job = output.strip()
        cancel_file.write(callback_params["callback_params"]["cancel_file_line"].format(sequence_id=sequence_id_job))
        cancel_file.flush()

    def get_submission_arguments(self, depend_job_ids):

        subprocess_args = []
        subprocess_args.append(self.submit_command)

        if depend_job_ids:
            assert isinstance(depend_job_ids, list)
            depend_string = "{}{}".format(self.depend_parameter, self.depend_separator.join(depend_job_ids))
            subprocess_args.append(depend_string)

        subprocess_args.append(self.submit_script)

        # Ensure that any spaces in the depend_string are handled correctly
        subprocess_args = ' '.join(subprocess_args).split(' ')

        return subprocess_args

    def run(self, depend_job_ids, multi_threading=True):
        """
        'Run' this job

            This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """

        self.executor.wait_until(self.start_delay)

        # get the submission params
        subprocess_args = self.get_submission_arguments(depend_job_ids)

        print "Submitting job {}".format(self.id)

        # multi-threaded option
        if multi_threading:

            self.executor.thread_manager.subprocess_callback(self.submission_callback, *subprocess_args)

        else:  # run in a subprocess and call the callback manually

            output = subprocess.check_output(subprocess_args)
            self.submission_callback(output)
