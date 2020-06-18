import math
import os

from kronos_executor.hpc import HPCJob


class SyntheticAppJob(HPCJob):

    needs_read_cache = True

    def __init__(self, job_config, executor, path):
        super(SyntheticAppJob, self).__init__(job_config, executor, path)

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
            'num_hyperthreads': 1,
            'job_name': 'synthApp_{}_{}_{}'.format(nprocs, nnodes, self.id)
        })
