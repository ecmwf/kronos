import os

from kronos_executor.hpc import HPCJob

class SyntheticAppJob(HPCJob):

    default_template = "synapp.sh"

    needs_read_cache = True

    def customised_generated_internals(self, script_format):
        """
        User-defined generation of parts of the submit script.
        :param script_format:
        :return:
        """
        script_format.update({
            'procs_per_node': min(self.executor.procs_per_node, script_format['num_procs']),
            'coordinator_binary': self.executor.coordinator_binary,
            'coordinator_library_path': os.path.join( os.path.dirname(self.executor.coordinator_binary),"../lib"),
            'job_name': 'synthApp_{}_{}_{}'.format(script_format['num_procs'], script_format['num_nodes'], self.id)
        })

Job = SyntheticAppJob
