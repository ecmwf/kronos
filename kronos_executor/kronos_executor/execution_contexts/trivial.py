
import pathlib

from kronos_executor.execution_context import ExecutionContext

run_script = pathlib.Path(__file__).parent / "trivial_run.sh"

class TrivialExecutionContext(ExecutionContext):

    scheduler_directive_start = ""
    scheduler_directive_params = {}
    scheduler_use_params = []
    scheduler_cancel_head = "#!/bin/bash\nkill "
    scheduler_cancel_entry = "{sequence_id} "

    launcher_command = "mpirun"
    launcher_params = {"num_procs": "-np "}
    launcher_use_params = ["num_procs"]

    def env_setup(self, job_config):
        return "module load openmpi"

    def submit_command(self, job_config, job_script_path, deps=[]):
        return [str(run_script),
                job_config['job_output_file'],
                job_config['job_error_file'],
                job_script_path]

Context = TrivialExecutionContext
