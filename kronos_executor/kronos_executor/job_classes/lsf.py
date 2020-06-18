from kronos_executor.synapp_job import SyntheticAppJob


job_template = """#!/bin/bash
#BSUB -L /bin/bash
#BSUB -J {job_name}
#BSUB -q queue_name
#BSUB -n {num_procs}
#BSUB -R "affinity[thread(2):cpubind=thread:distribute=pack] span[ptile=80] select[hname==p10a32 || hname==p10a47 || hname==p10a35 || hname==p10a38 || hname==p10a26 || hname==p10a44 || hname==p10a30 || hname==p10a46 || hname=p10a42 || hname==p10a43 || hname==p10a45 || hname==p10a52]"
#BSUB -o {job_output_file}
#BSUB -e {job_error_file}
#BSUB -H

# Configure the locations for the synthetic app to dump/load files in the i/o kernels
export KRONOS_WRITE_DIR="{write_dir}"
export KRONOS_READ_DIR="{read_dir}"
export KRONOS_SHARED_DIR="{shared_dir}"
export KRONOS_TOKEN="{simulation_token}"

export LD_LIBRARY_PATH={coordinator_library_path}:${{LD_LIBRARY_PATH}}

# Change to the original directory for submission
cd {job_dir}

# Export an EC simulation ID (to assist identifying the job in darshan logs)
export EC_simulation_id="{job_name}"

{profiling_code}

{launcher_command} -x LD_LIBRARY_PATH="${{LD_LIBRARY_PATH}}" -N {procs_per_node} -n {num_procs} {coordinator_binary} {input_file}
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

cancel_file_head = "#!/bin/sh\nbkill "
cancel_file_line = "{sequence_id} "


class LSFMixin(object):
    """
    Define the templates for LSF
    """

    submit_script_template = job_template
    ipm_template = ipm_template
    darshan_template = darshan_template
    allinea_template = allinea_template
    submit_command = "lsf_filter.sh"
    depend_parameter = "-w "
    depend_separator = ":"
    launcher_command = 'mpiexec'
    allinea_launcher_command = "map --profile aprun"
    allinea_lic_file_template = allinea_lic_file_template
    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line


class Job(LSFMixin, SyntheticAppJob):
    pass

