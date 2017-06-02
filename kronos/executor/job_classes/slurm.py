from kronos.executor.job_classes.hpc import HPCJob

#####################################################################################################
# This file is an example of a job submit template needed to run the executor on a HPC system. This template is
# called "slurm.py" and is an example template that can be used to submit jobs to a slurm scheduler. In order to
# instruct the executor to use this template, the following entry should be set in the executor configuration file:
#
# - "job_class": "slurm"
#
# Below, the details of how to setup the script are provided. This script can also be used as reference to generate user
# defined submit scripts (e.g. "user_job_script.py") that will then be invoked by setting the following entry in the
# executor config file:
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
#     experiment_id: "job-ID"
#
#     num_nodes: "N of nodes requested from the system. The Executor requests a number of nodes that is
#                 equal to N_processes (as specified in the KSF for each synthetic app) divided by the
#                 number of processors per node as specified in the executor config file"
#
#     num_procs: "N of processes requested
#
#     job_output_file: "job stdout file in the job folder"
#
#     job_error_file: "job error file in the job folder"
#
#     write_dir: "executor output folder"
#
#     read_dir: "folder containing the read files - automatically generated by Kronos before the run"
#
#     shared_dir: "auxiliary folder used for operations that the synthetic apps might perform on a shared folder
#                 like mkdir/rmdir (only used if these types of operations are present in the KSF file)"
#
#     job_dir: "job output directory"
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Variables to be manually set by the user
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# 2) In addition to the main template of the submit scripts, the following variables need to be MANUALLY set
#    in the template class below
#
#    submit_command: "submit command of the scheduler (e.g. "sbatch" for slurm)"
#
#    launcher_command: "launcher command of the HPC system"
#
#    cancel_file_head: "header of the "killjobs" bash script that Kronos automatically generates in the output folder"
#
#    cancel_file_line: "this is the list of job IDs that will be killed by the the killjobs script is invoked"
#
#                      - NB: the variable "sequence_id" with the list of job ID's is provided by Kronos
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

job_template = """#!/bin/sh

#SBATCH --job-name={experiment_id}
#SBATCH -N {num_nodes}
#SBATCH --ntasks={num_procs}
#SBATCH --cpus-per-task=1
#SBATCH --output={job_output_file}
#SBATCH --error={job_error_file}

# Configure the locations for the synthetic app to dump/load files in the i/o kernels
export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"

module load openmpi

# Change to the original directory for submission
cd {job_dir}

{profiling_code}

set -euxa

{launcher_command} -np {num_procs} {coordinator_binary} {input_file}
"""


ipm_template = """
# Configure IPM
module load ipm
export IPM_LOGDIR={job_dir}/ipm-logs
export IPM_HPM=PAPI_FP_OPS,PAPI_TOT_INS,PAPI_L1_DCM,PAPI_L2_DCM,PAPI_L3_DCM,PAPI_L1_DCA,PAPI_L1_TCM,PAPI_L2_TCM,PAPI_L3_TCM
"""

darshan_template = """
export DARSHAN_LOG_PATH={job_dir}
export LD_PRELOAD={darshan_lib_path}
"""

allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""

allinea_lic_file_template = """
export ALLINEA_LICENCE_FILE={allinea_licence_file}
"""


cancel_file_head = "#!/bin/sh\nscancel "
cancel_file_line = "{sequence_id} "


class SLURMMixin(object):
    """
    Define the templates for PBS
    """

    submit_script_template = job_template
    ipm_template = ipm_template
    darshan_template = darshan_template
    allinea_template = allinea_template
    allinea_lic_file_template = allinea_lic_file_template
        
    submit_command = "sbatch"
    depend_parameter = "--dependency=afterany:"
    depend_separator = ":"
    launcher_command = 'mpirun'
    allinea_launcher_command = "map --profile mpirun"

    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line


class Job(SLURMMixin, HPCJob):
    pass

