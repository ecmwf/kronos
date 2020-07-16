import math
import os

from kronos_executor.hpc import HPCJob
from kronos_executor.template_job import TemplateMixin

class SyntheticAppJob(TemplateMixin, HPCJob):

    default_template = "synapp.sh"

    needs_read_cache = True

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

Job = SyntheticAppJob
