
from kronos_executor.execution_context import ExecutionContext

class SlurmExecutionContext(ExecutionContext):

    scheduler_directive_start = "#SBATCH "
    scheduler_directive_params = {
        "job_name": "--job-name=",
        "num_nodes": "-N ",
        "num_procs": "--ntasks=",
        "cpus_per_task": "--cpus-per-task=",
        "job_output_file": "--output=",
        "job_error_file": "--error="
    }
    scheduler_use_params = [
        'job_name', 'num_nodes', 'num_procs', 'cpus_per_task', 'job_output_file', 'job_error_file']

    submit_command_ = "sbatch"
    submit_job_dependency = "--dependency=afterany:"
    submit_dependency_separator = ":"

    launcher_command = "mpirun"
    launcher_params = {"num_procs": "-np "}
    launcher_use_params = ["num_procs"]

    def env_setup(self, job_config):
        return "module load openmpi"


Context = SlurmExecutionContext
