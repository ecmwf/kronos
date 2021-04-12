
from kronos_executor.tools import load_module

class ExecutionContext:
    """Handles the interaction between jobs and the system"""

    #: Prefix for scheduler directives (e.g. ``"#SBATCH "``)
    scheduler_directive_start = None
    #: Directive parameter identifiers, keys should match the job config passed
    #: to `scheduler_params`, e.g. ``"job_name": "--job-name="``
    scheduler_directive_params = {}
    #: Parameters to use, from `scheduler_directive_params`
    scheduler_use_params = [
        'job_name', 'num_procs', 'num_nodes', 'job_output_file', 'job_error_file']
    #: Script to cancel all submitted jobs: start of file
    scheduler_cancel_head = None
    #: ``format()`` string for an entry in the cancel script, job id passed as sequence_id
    scheduler_cancel_entry = None

    #: Command to submit a job, e.g. ``"sbatch"``
    submit_command_ = None
    #: Parameter to specify dependencies between_jobs
    #: (e.g. ``"--dependency=afterany:"``)
    submit_job_dependency = None
    #: Separator for a list of dependencies (e.g. ``":"``)
    submit_dependency_separator = None

    #: Parallel launcher command (e.g. ``"mpirun"``, ``"srun"``)
    launcher_command = None
    #: Launcher parameter identifiers, keys should match the job config passed
    #: to `launch_command`, e.g. ``"num_procs": "-np "``
    launcher_params = {}
    #: Parameters to use, from `launcher_params`
    launcher_use_params = []

    def __init__(self, config):
        self.config = config.copy()

    def scheduler_params(self, job_config, use_params=None):
        """Format the appropriate scheduler directives for the given job
        :param job_config: job configuration (dict)
        :param use_params: (optional) if set, use these instead of `scheduler_use_params` (list)"""

        assert self.scheduler_directive_start is not None

        lines = []

        if use_params is None:
            use_params = self.scheduler_use_params

        for pname in use_params:
            param = self.scheduler_directive_params.get(pname)
            if param is not None:
                pval = None
                if pname in job_config:
                    pval = job_config[pname]
                else:
                    pval = self.config[pname]
                lines.append(self.scheduler_directive_start + param + str(pval))

        return "\n".join(lines)

    def env_setup(self, job_config):
        """Generate environment setup lines for the given job"""
        return ""

    def launch_command(self, job_config, use_params=None):
        """Generate the command line to launch the job from the job script
        :param job_config: job configuration (dict)
        :param use_params: (optional) if set, use these instead of `launcher_use_params` (list)"""

        launcher_command = job_config.get('launcher_command',
            self.launcher_command)
        assert launcher_command is not None

        command = [launcher_command]

        if use_params is None:
            use_params = self.launcher_use_params

        for pname in use_params:
            param = self.launcher_params.get(pname)
            if param is not None:
                pval = None
                if pname in job_config:
                    pval = job_config[pname]
                else:
                    pval = self.config[pname]
                command.append(param + str(pval))

        return " ".join(command)

    def submit_command(self, job_config, job_script_path, deps=[]):
        """Generate the command line to submit the job"""
        assert self.submit_command_ is not None

        command = [self.submit_command_]

        if deps and self.submit_job_dependency is not None and \
            self.submit_dependency_separator is not None:

            deps_str = "{}{}".format(self.submit_job_dependency,
                self.submit_dependency_separator.join(deps))
            command.append(deps_str)

        command.append(job_script_path)
        return command

    def cancel_entry(self, sequence_id, first):
        """Generate a new entry to append to the cancel file"""
        entry = ""
        if first:
            entry += self.scheduler_cancel_head
        entry += self.scheduler_cancel_entry.format(sequence_id=sequence_id)
        return entry


def load_context(name, path, config):
    """Load the Context object from a module named ``name`` to be found in one
    of the directories in ``path``"""

    try:
        mod = load_module(name, path, prefix="kronos_execution_context_")
    except RuntimeError:
        raise ValueError("Execution context {!r} not found in paths {}".format(name, ", ".join(path)))

    if not hasattr(mod, 'Context'):
        raise RuntimeError(
            "Execution context {!r} does not define a Context class" \
            .format(name))

    return mod.Context(config)
