import os
import stat
import subprocess

from kronos_executor.base_job import BaseJob


class HPCJob(BaseJob):

    submit_script_template = None

    cancel_file = None
    cancel_file_head = None
    cancel_file_line = None

    def __init__(self, job_config, executor, path):
        super(HPCJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")
        self.output_file = os.path.join(self.path, "output")
        self.error_file = os.path.join(self.path, "error")

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
            'job_output_file': self.output_file,
            'job_error_file': self.error_file,
            'simulation_token': self.executor.simulation_token
        }

        self.customised_generated_internals(script_format)

        script_format.setdefault('launcher_command', self.executor.execution_context.launcher_command)
        script_format.setdefault('scheduler_params', self.executor.execution_context.scheduler_params(script_format))
        script_format.setdefault('env_setup', self.executor.execution_context.env_setup(script_format))
        script_format.setdefault('launch_command', self.executor.execution_context.launch_command(script_format))

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
        sequence_id_job = output.strip()
        self.executor.set_job_submitted(self.id, sequence_id_job)

    def get_submission_arguments(self, depend_job_ids):
        config = self.job_config.copy()
        config['job_output_file'] = self.output_file
        config['job_error_file'] = self.error_file
        return self.executor.execution_context.submit_command(
            config, self.submit_script, depend_job_ids)
