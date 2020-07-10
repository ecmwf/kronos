
from kronos_executor.execution_context import ExecutionContext

class PBSExecutionContext(ExecutionContext):

    scheduler_directive_start = "#PBS "
    scheduler_directive_params = {
        "job_name": "-N ",
        "num_nodes": "-l EC_nodes=",
        "num_procs": "-l EC_total_tasks=",
        "cpus_per_task": "-l EC_threads_per_task=",
        "num_hyperthreads": "-l EC_hyperthreads=",
        "job_output_file": "-o ",
        "job_error_file": "-e ",
        "scheduler_queue": "-q "
    }
    scheduler_use_params = [
        'job_name', 'scheduler_queue', 'num_nodes', 'num_procs',
        'cpus_per_task', 'num_hyperthreads', 'job_output_file',
        'job_error_file']

    submit_command_ = "qsub"
    submit_job_dependency = "-W dependency=afterany:"
    submit_dependency_separator = ":"

    launcher_command = "aprun"
    launcher_params = {
        "procs_per_node": "-N ",
        "num_procs": "-n ",
        "export_library_path": "-e "
    }
    launcher_use_params = ["export_library_path", "procs_per_node", "num_procs"]

    def __init__(self, config):
        super().__init__(config)
        self.config['scheduler_queue'] = 'np'
        self.config['export_library_path'] = 'LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"'

    def env_setup(self, job_config):
        return  """\
export LD_LIBRARY_PATH={coordinator_library_path}:${{LD_LIBRARY_PATH}}

# Export an EC simulation ID (to assist identifying the job in darshan logs)
export EC_simulation_id="{job_name}"\
""".format(**job_config)


Context = PBSExecutionContext
