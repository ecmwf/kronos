
import pathlib

from kronos_executor.external_job import UserAppJob


class Job(UserAppJob):

    default_template = "stepper_job.sh"

    needed_config_params = [
        "nsteps",
        "size_kb",
    ]

    def customised_generated_internals(self, script_format):
        """
        Function used to define an environment variable generated from configuration parameters
        """
        super().customised_generated_internals(script_format)

        here = pathlib.Path(__file__).parent

        # Load the conda environment
        env_script_default = here / "environment.sh"
        env_script = pathlib.Path(self.executor.config.get("environment_script", env_script_default))
        if env_script.is_file():
            script_format['source_kronos_env'] = "set +u; source {}; set -u".format(env_script)
        else:
            script_format['source_kronos_env'] = ""

        # Path to the external app
        script_format['stepper'] = "python {}".format(here / "stepper.py")
