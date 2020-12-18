import os
from kronos_executor.external_job import UserAppJob

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

