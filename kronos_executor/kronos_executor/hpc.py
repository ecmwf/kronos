import jinja2
import math
import os
import stat

from kronos_executor.base_job import BaseJob


class HPCJob(BaseJob):

    default_template = None

    cancel_file = None
    cancel_file_head = None
    cancel_file_line = None

    def __init__(self, job_config, executor, path):
        super(HPCJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")
        self.output_file = os.path.join(self.path, "output")
        self.error_file = os.path.join(self.path, "error")

        template_loader = jinja2.ChoiceLoader([
            jinja2.FileSystemLoader(os.getcwd()),
            jinja2.FileSystemLoader(executor.config.get('job_templates_path', [])),
            jinja2.PackageLoader('kronos_executor', 'job_templates')
        ])
        self.template_env = jinja2.Environment(loader=template_loader)

        self.job_template_name = job_config.get("job_template", self.default_template)
        if self.job_template_name is None:
            raise ValueError(
                "No default template for job class {} and no 'job_template' specified".format(job_config['job_class']))

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

        default_nprocs = self.job_config.get('num_procs', 1)
        default_nnodes = int(math.ceil(float(default_nprocs) / self.executor.procs_per_node))

        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'shared_dir': self.executor.job_dir_shared,
            'input_file': self.input_file,
            'job_dir': self.path,
            'job_name': self.job_config.get('job_name', 'kron-{}'.format(self.id)),
            'job_num': self.id,
            'job_output_file': self.output_file,
            'job_error_file': self.error_file,
            'num_procs': default_nprocs,
            'num_nodes': default_nnodes,
            'cpus_per_task': 1,
            'num_hyperthreads': 1,
            'simulation_token': self.executor.simulation_token
        }

        self.customised_generated_internals(script_format)

        script_format.setdefault('launcher_command', self.executor.execution_context.launcher_command)
        script_format.setdefault('scheduler_params', self.executor.execution_context.scheduler_params(script_format))
        script_format.setdefault('env_setup', self.executor.execution_context.env_setup(script_format))
        script_format.setdefault('launch_command', self.executor.execution_context.launch_command(script_format))

        template = self.template_env.get_template(self.job_template_name)
        stream = template.stream(script_format)
        with open(self.submit_script, 'w') as f:
            stream.dump(f)

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def submission_callback(self, output):
        """
        This is the callback function
        """
        sequence_id_job = output.decode("utf-8").strip()
        self.executor.set_job_submitted(self.id, sequence_id_job)

    def get_submission_arguments(self, depend_job_ids):
        config = self.job_config.copy()
        config['job_output_file'] = self.output_file
        config['job_error_file'] = self.error_file
        return self.executor.execution_context.submit_command(
            config, self.submit_script, depend_job_ids)
