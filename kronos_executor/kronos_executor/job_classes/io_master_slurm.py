import os
from kronos_executor.job_classes.external_app import UserAppJob

# ==================================================================
# Very minimal example of io_master application with SLURM scheduler
# ==================================================================

job_template = """#!/bin/sh
{scheduler_params}

export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"
export KRONOS_TOKEN="{simulation_token}"

{env_setup}

# Call the io master and configure it with the appropriate I/O tasks in the time_schedule
{kronos_bin_dir}/remote_io_master {ioserver_hosts_file} {input_file}

sleep 5

{send_complete_msg}

"""

class Job(UserAppJob):

    submit_script_template = job_template

    needed_config_params = [
        "tasks"
    ]

    def __init__(self, job_config, executor, path):
        super(Job, self).__init__(job_config, executor, path)
        assert self.executor.execution_context is not None

    def customised_generated_internals(self, script_format):
        """
        Function used to define an environment variable generated from configuration parameters
        """
        super().customised_generated_internals(script_format)

        script_format['ioserver_hosts_file'] = self.executor.ioserver_hosts_file

        # ===== SETUP to send TCP message to kronos at the end of job ======
        bin_dir = os.path.dirname(self.executor.coordinator_binary)
        send_msg_exe = "{}/send_job_complete_msg.py".format(bin_dir)

        send_msg_text = "{} {} {}".format(self.executor.notification_host,
                                          self.executor.notification_port,
                                          self.id)

        script_format['send_complete_msg'] = "python {} {}".format(send_msg_exe, send_msg_text)
        # ==================================================================

