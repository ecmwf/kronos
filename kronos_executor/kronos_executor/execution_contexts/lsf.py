
from kronos_executor.execution_context import ExecutionContext

class LSFExecutionContext(ExecutionContext):

    scheduler_directive_start = "#BSUB "
    scheduler_directive_params = {
        "login_shell": "-L ",
        "job_name": "-J ",
        "num_procs": "-n ",
        "job_output_file": "-o ",
        "job_error_file": "-e ",
        "scheduler_queue": "-q ",
        "scheduler_resources": "-R ",
        "hold_job": "-H"
    }
    scheduler_use_params = [
        'login_shell', 'job_name', 'scheduler_queue', 'num_procs',
        'scheduler_resources', 'job_output_file', 'job_error_file', 'hold_job']

    scheduler_cancel_head = "#!/bin/sh\nbkill "
    scheduler_cancel_entry = "{sequence_id} "

    submit_command_ = "lsf_filter.sh"
    submit_job_dependency = "-w "
    submit_dependency_separator = ":"

    launcher_command = "mpiexec"
    launcher_params = {
        "procs_per_node": "-N ",
        "num_procs": "-n ",
        "export_library_path": "-x "
    }
    launcher_use_params = ["export_library_path", "procs_per_node", "num_procs"]

    def __init__(self, config):
        super().__init__(config)
        self.config['login_shell'] = '/bin/bash'
        self.config['scheduler_queue'] = 'queue_name'
        self.config['scheduler_resources'] = '"affinity[thread(2):cpubind=thread:distribute=pack] span[ptile=80] select[hname==p10a32 || hname==p10a47 || hname==p10a35 || hname==p10a38 || hname==p10a26 || hname==p10a44 || hname==p10a30 || hname==p10a46 || hname=p10a42 || hname==p10a43 || hname==p10a45 || hname==p10a52]"'
        self.config['export_library_path'] = 'LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"'
        self.config['hold_job'] = ""

    def env_setup(self, job_config):
        return  """\
export LD_LIBRARY_PATH={coordinator_library_path}:${{LD_LIBRARY_PATH}}

# Export an EC simulation ID (to assist identifying the job in darshan logs)
export EC_simulation_id="{job_name}"\
""".format(**job_config)


Context = LSFExecutionContext
