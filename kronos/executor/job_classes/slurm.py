from kronos.executor.job_classes.hpc import HPCJob


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

allinea_template = """
# Configure Allinea Map
export PATH={allinea_path}:${{PATH}}
export LD_LIBRARY_PATH={allinea_ld_library_path}:${{LD_LIBRARY_PATH}}
"""

allinea_lic_file_template = """
export ALLINEA_LICENCE_FILE={allinea_licence_file}
"""



cancel_file_head = "#!/bin/sh\nqdel "
cancel_file_line = "{sequence_id} "

class SLURMMixin(object):
    """
    Define the templates for PBS
    """

    submit_script_template = job_template
    ipm_template = ipm_template
    allinea_template = allinea_template
    allinea_lic_file_template = allinea_lic_file_template
        
    submit_command = "sbatch"
    launcher_command = 'mpirun'
    allinea_launcher_command = "map --profile mpirun"

    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line


class Job(SLURMMixin, HPCJob):
    pass

