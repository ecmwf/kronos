
import os

from kronos_executor.external_job import UserAppJob


job_template_trivial = """
#!/bin/bash

wdir="{write_dir}"
cd $wdir

# kronos simulation token
export KRONOS_TOKEN="{simulation_token}"

{source_kronos_env}

nsteps={nsteps}

for step in $(seq 1 $nsteps) ; do
    echo "Running step $step"
    {stepper} $step {size_kb} "{shared_dir}"
    sleep 10
    {notify_script} --type="NotifyMetadata" stepper "{{\\"step\\": $step}}"
done

# send end-of-job msg
{notify_script} --type="Complete" stepper
"""


class TrivialMixin(object):
    submit_command = os.path.join(os.path.dirname(__file__), "run.sh")
    submit_script_template = job_template_trivial
    load_env = False

    def get_submission_arguments(self, depend_job_ids):
        subprocess_args = super(TrivialMixin, self).get_submission_arguments(depend_job_ids)
        outfile = os.path.join(self.path, "output")
        errfile = os.path.join(self.path, "error")
        subprocess_args.insert(1, outfile)
        subprocess_args.insert(2, errfile)
        return subprocess_args


job_template_pbs = """
#!/bin/bash
#PBS -N {job_name}
#PBS -q ns
#PBS -l walltime=00:30:00
#PBS -o {job_output_file}
#PBS -e {job_error_file}

wdir="{write_dir}"
cd $wdir

# kronos simulation token
export KRONOS_TOKEN="{simulation_token}"

{source_kronos_env}

nsteps={nsteps}

for step in $(seq 1 $nsteps) ; do
    echo "Running step $step"
    {stepper} $step {size_kb} "{shared_dir}"
    sleep 10
    {notify_script} --type="NotifyMetadata" stepper "{{\\"step\\": $step}}"
done

# send end-of-job msg
{notify_script} --type="Complete" stepper
"""


class PBSMixin(object):
    submit_script_template = job_template_pbs
    submit_command = "qsub"
    depend_parameter = "-W depend=afterany:"
    depend_separator = ":"
    launcher_command = 'aprun'
    load_env = True


class Job(TrivialMixin, UserAppJob):

    needed_config_params = [
        "nsteps",
        "size_kb",
    ]

    def customised_generated_internals(self, script_format):
        """
        Function used to define an environment variable generated from configuration parameters
        """

        here = os.path.dirname(__file__)

        # Load the conda environment
        env_script_default = os.path.join(here, "environment.sh")
        env_script = self.executor.config.get("environment_script", env_script_default)
        if self.load_env:
            script_format['source_kronos_env'] = "set +u; source {}; set -u".format(env_script)
        else:
            script_format['source_kronos_env'] = ""

        # Utility to send an event to kronos
        script_format['notify_script'] = "python {} {} {} {}".format(
                os.path.join(here, "notify.py"),
                self.executor.notification_host,
                self.executor.notification_port,
                self.id)

        # Path to the external app
        script_format['stepper'] = "python {}".format(os.path.join(here, "stepper.py"))
