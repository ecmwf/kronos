from job_classes.hpc import HPCJob


job_template = """#!/bin/sh
#PBS -N {experiment_id}
#PBS -q {queue}
#PBS -l EC_nodes={num_nodes}
#PBS -l EC_total_tasks={num_procs}
#PBS -l EC_threads_per_task=1
#PBS -l EC_hyperthreads={num_hyperthreads}
#PBS -o {job_output_file}
#PBS -e {job_error_file}

# Configure the locations for the synthetic app to dump/load files in the i/o kernels
export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"

# Change to the original directory for submission
cd {job_dir}

# Export an EC experiment ID (to assist identifying the job in darshan logs)
export EC_experiment_id="{experiment_id}"

{profiling_code}

{launcher_command} -N {num_procs} -n {num_procs} {coordinator_binary} {input_file}
"""


ipm_template = """
# Configure IPM
module load ipm
export IPM_LOGDIR={job_dir}/ipm-logs
export IPM_HPM=PAPI_FP_OPS,PAPI_TOT_INS,PAPI_L1_DCM,PAPI_L2_DCM,PAPI_L3_DCM,PAPI_L1_DCA,PAPI_L1_TCM,PAPI_L2_TCM,PAPI_L3_TCM
"""


allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""


cancel_file_head = "#!/bin/sh\nqdel "
cancel_file_line = "{sequence_id} "

class PBSMixin(object):
    """
    Define the templates for PBS
    """

    submit_script_template = job_template
    ipm_template = ipm_template
    allinea_template = allinea_template
    submit_command = "qsub"
    launcher_command = 'aprun'
    allinea_launcher_command = "map --profile aprun"

    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line


class Job(PBSMixin, HPCJob):
    pass

