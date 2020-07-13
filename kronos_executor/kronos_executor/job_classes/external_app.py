
import os

from kronos_executor.external_job import UserAppJob

job_template = """#!/bin/sh
{scheduler_params}

# kronos simulation token (automatically set by Kronos - do not edit)
export KRONOS_TOKEN="{simulation_token}"

{env_setup}

echo {name_foo_1} {name_foo_2}

# send end-of-job msg (formed in the function "customised_generated_internals" belowi - no need to edit)
{source_kronos_env}
{send_complete_msg}

"""


class Job(UserAppJob):

    submit_script_template = job_template

    needed_config_params = [
        "name_foo_1",
        "name_foo_2"
     ]

    def customised_generated_internals(self, script_format):
        """
        Function used to define an environment variable generated from configuration parameters
        """
        super().customised_generated_internals(script_format)

        # ===== SETUP to send TCP message to kronos at the end of job ======
        inst_dir = "{}".format(os.path.join(self.executor.executor_file_dir, "../../../"))
        script_format['source_kronos_env'] = "set +u; source {}; set -u".format(os.path.join(inst_dir, "environment.sh"))

        send_msg_exe = os.path.join(inst_dir, "send_job_complete_msg.py")
        send_msg_text = "{} {} {}".format(self.executor.notification_host, self.executor.notification_port, self.id)
        script_format['send_complete_msg'] = "python {} {}".format(send_msg_exe, send_msg_text)
        # ==================================================================

