import math
import os
import stat

from kronos.executor.job_classes.base_job import BaseJob


class UserAppJob(BaseJob):

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

    needed_config_params = None

    def __init__(self, job_config, executor, path):
        super(UserAppJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")

    def check_job_config(self):
        """
        Make sure that all the parameters needed are in the config list
        :return:
        """

        if not self.needed_config_params:
            raise KeyError("No parameters to be passed to the job submit template have been defined!")

        for config_param in self.needed_config_params:
            assert config_param in self.job_config["config_params"]

    def customised_generated_internals(self, script_format):
        """
        Place-holder for user-defined generation of parts of the submit script..
        For instance, can be used for composing an environment variable to be passed to the
        user-defined-application prior to the run. It can easily combine kschedule config
        parameters and kronos executor configuration parameters.
        :param script_format:
        :return:
        """
        pass

    def generate_internal(self):

        # update the template with the config parameters
        script_format = {
            'job_name': 'kronos-app-{}'.format(self.id),
            'job_num': self.id,
            'job_output_file': os.path.join(self.path, "output"),
            'job_error_file': os.path.join(self.path, "error"),
            'launcher_command': self.launcher_command,
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'input_file': self.input_file
        }

        # place-holder for user-defined generation of parts of the submit script..
        self.customised_generated_internals(script_format)

        # update the job submit template with all the configs

        print self.job_config["config_params"]
        for param_name in self.needed_config_params:
            script_format.update({param_name: self.job_config["config_params"][param_name]})

        with open(self.submit_script, 'w') as f:
            f.write(self.submit_script_template.format(**script_format))

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def submission_callback(self, output):
        """
        This is the callback function
        """
        if UserAppJob.cancel_file is None:
            cancel_file_path = os.path.join(self.executor.job_dir, "killjobs")
            UserAppJob.cancel_file = open(cancel_file_path, 'w')
            UserAppJob.cancel_file.write(self.cancel_file_head)
            os.chmod(cancel_file_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

        # sequence_id_job = filter(str.isdigit, output)
        sequence_id_job = output.strip()
        UserAppJob.cancel_file.write(self.cancel_file_line.format(sequence_id=sequence_id_job))
        UserAppJob.cancel_file.flush()

        self.executor.set_job_submitted(self.id, sequence_id_job)

    def run(self, depend_job_ids):
        """
        'Run' this job

            This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """

        self.executor.wait_until(self.start_delay)

        subprocess_args = []
        subprocess_args.append(self.submit_command)

        if depend_job_ids:
            assert isinstance(depend_job_ids, list)
            depend_string = "{}{}".format(self.depend_parameter, self.depend_separator.join(depend_job_ids))
            subprocess_args.append(depend_string)

        subprocess_args.append(self.submit_script)

        # Ensure that any spaces in the depend_string are handled correctly
        subprocess_args = ' '.join(subprocess_args).split(' ')

        print "Submitting job {}".format(self.id)
        self.executor.thread_manager.subprocess_callback(self.submission_callback, *subprocess_args)
