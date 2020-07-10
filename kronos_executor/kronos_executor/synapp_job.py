import math
import os

from kronos_executor.hpc import HPCJob
from kronos_executor.template_job import TemplateMixin

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


class SyntheticAppJob(TemplateMixin, HPCJob):

    default_template = "synapp.sh"

    needs_read_cache = True

    ipm_template = ipm_template
    darshan_template = darshan_template
    allinea_template = allinea_template
    allinea_lic_file_template = allinea_lic_file_template

    def __init__(self, job_config, executor, path):
        super(SyntheticAppJob, self).__init__(job_config, executor, path)
        assert self.executor.execution_context is not None

    def customised_generated_internals(self, script_format):
        """
        User-defined generation of parts of the submit script.
        :param script_format:
        :return:
        """
        nprocs = self.job_config.get('num_procs', 1)
        nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))
        script_format.update({
            'procs_per_node': min(self.executor.procs_per_node, nprocs),
            'coordinator_binary': self.executor.coordinator_binary,
            'coordinator_library_path': os.path.join( os.path.dirname(self.executor.coordinator_binary),"../lib"),
            'num_procs': nprocs,
            'num_nodes': nnodes,
            'cpus_per_task': 1,
            'num_hyperthreads': 1,
            'job_name': 'synthApp_{}_{}_{}'.format(nprocs, nnodes, self.id)
        })
