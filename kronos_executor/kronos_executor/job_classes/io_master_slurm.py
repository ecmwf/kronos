import os
from kronos_executor.job_classes.external_app import UserAppJob

# ==================================================================
# Very minimal example of io_master application with SLURM scheduler
# ==================================================================

job_template = """#!/bin/sh

# ---------- SLURM -------------
#SBATCH --job-name={job_name}
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output={job_output_file}
#SBATCH --error={job_error_file}

export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"
export KRONOS_TOKEN="{simulation_token}"

# Call the io master and configure it with the appropriate I/O tasks in the time_schedule
{kronos_bin_dir}/remote_io_master <HOST_FILE> {tasks}

sleep 5

{send_complete_msg}

"""

cancel_file_head = "#!/bin/sh\nscancel "
cancel_file_line = "{sequence_id} "


class SLURMMixin(object):
    """
    Define the templates for PBS
    """

    submit_script_template = job_template
    submit_command = "sbatch"
    depend_parameter = "--dependency=afterany:"
    depend_separator = ":"
    launcher_command = 'mpirun'
    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line

    needed_config_params = [
        "tasks"
     ]


class Job(SLURMMixin, UserAppJob):

    def customised_generated_internals(self, script_format):
        """
        Function used to define an environment variable generated from configuration parameters
        """

        # ===== SETUP to send TCP message to kronos at the end of job ======
        bin_dir = os.path.dirname(self.executor.coordinator_binary)
        send_msg_exe = "{}/send_job_complete_msg.py".format(bin_dir)

        send_msg_text = "{} {} {}".format(self.executor.notification_host,
                                          self.executor.notification_port,
                                          self.id)

        script_format['send_complete_msg'] = "python {} {}".format(send_msg_exe, send_msg_text)
        # ==================================================================

