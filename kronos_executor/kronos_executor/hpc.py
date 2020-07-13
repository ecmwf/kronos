import os
import stat
import subprocess

from kronos_executor.base_job import BaseJob


class HPCJob(BaseJob):

    submit_script_template = None
    submit_command = None
    depend_parameter = None
    depend_separator = None
    launcher_command = None

    cancel_file = None
    cancel_file_head = None
    cancel_file_line = None

    def __init__(self, job_config, executor, path):
        super(HPCJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")

    def customised_generated_internals(self, script_format):
        """
        Place-holder for user-defined generation of parts of the submit script.
        For instance, can be used for composing an environment variable to be passed to the
        user-defined-application prior to the run. It can easily combine kschedule config
        parameters and kronos kronos_executor configuration parameters.
        :param script_format:
        :return:
        """
        pass

    def generate_internal(self):

        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'input_file': self.input_file,
            'job_dir': self.path,
            'job_name': 'kron-{}'.format(self.id),
            'job_num': self.id,
            'job_output_file': os.path.join(self.path, "output"),
            'job_error_file': os.path.join(self.path, "error"),
            'launcher_command': self.launcher_command,
            'simulation_token': self.executor.simulation_token
        }

        self.customised_generated_internals(script_format)

        if self.executor.execution_context is not None:
            script_format.setdefault('launcher_command', self.executor.execution_context.launcher_command)
            script_format.setdefault('scheduler_params', self.executor.execution_context.scheduler_params(script_format))
            script_format.setdefault('env_setup', self.executor.execution_context.env_setup(script_format))
            script_format.setdefault('launch_command', self.executor.execution_context.launch_command(script_format))
        else:
            script_format.setdefault('launcher_command', self.launcher_command)

        self.generate_script(script_format)

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def generate_script(self, script_format):
        assert self.submit_script_template is not None

        with open(self.submit_script, 'w') as f:
            f.write(self.submit_script_template.format(**script_format))

    def submission_callback(self, output):
        """
        This is the callback function
        """

        if self.executor.execution_context is None:
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
        Static function that does the callback (everything but informing the kronos_executor of the
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

        if self.executor.execution_context is not None:
            return self.executor.execution_context.submit_command(
                self.job_config, self.submit_script, depend_job_ids)

        subprocess_args = []
        assert self.submit_command is not None
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

        print("Submitting job {}".format(self.id))

        # multi-threaded option
        if multi_threading:

            self.executor.thread_manager.subprocess_callback(self.submission_callback, *subprocess_args)

        else:  # run in a subprocess and call the callback manually

            output = subprocess.check_output(subprocess_args)
            self.submission_callback(output)
