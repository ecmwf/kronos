import os
from kronos_executor.job_classes.external_app import UserAppJob

# =================================================
# Very minimal example of the io_master application
# =================================================

class Job(UserAppJob):

    default_template = "io_master.sh"

    needed_config_params = [
        "tasks"
    ]

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

