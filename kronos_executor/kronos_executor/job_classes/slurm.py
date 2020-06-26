from kronos_executor.synapp_job import SyntheticAppJob

#####################################################################################################
# This file is an example of a job submit template needed to run the kronos_executor on a HPC system. This template is
# called "slurm.py" and is an example template that can be used to submit jobs to a slurm scheduler. In order to
# instruct the kronos_executor to use this template, the following entry should be set in the kronos_executor configuration file:
#
# - "job_class": "slurm"
#
# Below, the details of how to setup the script are provided. This script can also be used as reference to generate user
# defined submit scripts (e.g. "user_job_script.py") that will then be invoked by setting the following entry in the
# kronos_executor config file:
#
# - "job_class": "user_job_script"
#
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Template fields automatically set by Kronos
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# 1) This section briefly describes the fields that are used by Kronos Executor to automatically populate
#    the template below ("submit_script_template"). The rest of the template needs to be adapted to the host HPC system
#    and scheduler as appropriate
#
#     job_name: "job-ID"
#
#     num_nodes: "Number of nodes requested from the system. The Executor requests a number of nodes that is
#                 equal to N_processes (as specified in the KSchedule for each synthetic app) divided by the
#                 number of processors per node as specified in the kronos_executor config file"
#
#     num_procs: "Number of MPI ranks requested
#
#     job_output_file: "job stdout file in the job folder"
#
#     job_error_file: "job error file in the job folder"
#
#     write_dir: "kronos_executor output folder"
#
#     read_dir: "folder containing the read files - automatically generated by Kronos before the run"
#
#     shared_dir: "auxiliary folder used for operations that the synthetic apps might perform on a shared folder
#                 like mkdir/rmdir (only used if these types of operations are present in the KSchedule file)"
#
#     job_dir: "job output directory"
#
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Variables to be manually set by the user
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# 2) In addition to the main template of the submit scripts, the following variables need to be MANUALLY set
#    in the template class below
#
#    submit_command: "The command used to submit jobs to the scheduler (e.g. "qsub" for PBS)"
#
#    launcher_command: "Command to begin the parallel part of the job (e.g. "aprun" for PBS on cray systems)"
#
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Other Variables - ONLY RELEVANT IF SCHEDULER DEPENDENCIES ARE TO BE USED (not the default Kronos option)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#    depend_parameter: "The argument used to specify job dependencies to the scheduler on the command line.
#                      (e.g. "-W depend=afterany:" for PBS)."
#
#    depend_separator = "separator used when constructing the job dependency list for submission to the
#                       scheduler (e.g. for pbs use ":")"
#
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Other Variables
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# The following variables can also be specified if the synthetic apps are to be profiled,
# but their usage is momentarily *unsupported*:
# ipm_template: IPM code
# darshan_template: Darshan code
# allinea_template, allinea_lic_file_template: Allinea MAP code
#
#####################################################################################################


cancel_file_head = "#!/bin/sh\nscancel "
cancel_file_line = "{sequence_id} "


class SLURMMixin:
    """
    Define the templates for PBS
    """

    submit_command = "sbatch"
    depend_parameter = "--dependency=afterany:"
    depend_separator = ":"
    launcher_command = 'mpirun'
    allinea_launcher_command = "map --profile mpirun"

    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line

    def customised_generated_internals(self, script_format):
        super(SLURMMixin, self).customised_generated_internals(script_format)

        script_format['scheduler_params'] = """\
#SBATCH --job-name={job_name}
#SBATCH -N {num_nodes}
#SBATCH --ntasks={num_procs}
#SBATCH --cpus-per-task=1
#SBATCH --output={job_output_file}
#SBATCH --error={job_error_file}\
""".format(**script_format)
        script_format['env_setup'] = "module load openmpi"
        script_format['launch_command'] = "{launcher_command} -np {num_procs}".format(**script_format)


class Job(SLURMMixin, SyntheticAppJob):
    pass

