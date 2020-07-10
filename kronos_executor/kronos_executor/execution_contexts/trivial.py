
from kronos_executor.execution_context import ExecutionContext

class TrivialExecutionContext(ExecutionContext):

    scheduler_directive_start = ""
    scheduler_directive_params = {}
    scheduler_use_params = []
    scheduler_cancel_head = ""
    scheduler_cancel_entry = ""

    launcher_command = "mpirun"
    launcher_params = {"num_procs": "-np "}
    launcher_use_params = ["num_procs"]

    def env_setup(self, job_config):
        return "module load openmpi"

    def submit_command(self, job_config, job_script_path, deps=[]):
        return ["bash", self.submit_script]

Context = TrivialExecutionContext
