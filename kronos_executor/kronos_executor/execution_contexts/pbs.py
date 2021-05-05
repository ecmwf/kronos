
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
        "scheduler_queue": "-q ",
        "login_shell": "-S ",
        "time_limit": "-l walltime="
    }
    scheduler_use_params = [
        'job_name', 'scheduler_queue', 'num_nodes', 'num_procs',
        'cpus_per_task', 'num_hyperthreads', 'job_output_file',
        'job_error_file']

    scheduler_cancel_head = "#!/bin/sh\nqdel "
    scheduler_cancel_entry = "{sequence_id} "

    submit_command_ = "qsub"
    submit_job_dependency = "-W dependency=afterany:"
    submit_dependency_separator = ":"

    launcher_command = "aprun"
    launcher_params = {
        "procs_per_node": "-N ",
        "num_procs": "-n ",
        "cpus_per_task": "-d ",
        "num_hyperthreads": "-j ",
        "export_library_path": "-e ",
        "cpu_binding": "-cc ",
        "strict_memory": "-ss"
    }
    launcher_use_params = ["export_library_path", "procs_per_node", "num_procs"]

    def __init__(self, config):
        super().__init__(config)
        self.config['scheduler_queue'] = 'np'
        self.config['export_library_path'] = 'LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"'

    def env_setup(self, job_config):
        libpath = ""
        if "coordinator_library_path" in job_config:
            libpath = """\
export LD_LIBRARY_PATH={coordinator_library_path}:${{LD_LIBRARY_PATH}}

"""
        return (libpath + """\
# Export an EC simulation ID (to assist identifying the job in darshan logs)
export EC_simulation_id="{job_name}"\
""").format(**job_config)


Context = PBSExecutionContext
