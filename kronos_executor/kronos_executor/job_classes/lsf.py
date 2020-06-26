from kronos_executor.synapp_job import SyntheticAppJob


cancel_file_head = "#!/bin/sh\nbkill "
cancel_file_line = "{sequence_id} "


class LSFMixin:
    """
    Define the templates for LSF
    """

    submit_command = "lsf_filter.sh"
    depend_parameter = "-w "
    depend_separator = ":"
    launcher_command = 'mpiexec'
    allinea_launcher_command = "map --profile aprun"
    cancel_file_head = cancel_file_head
    cancel_file_line = cancel_file_line

    def customised_generated_internals(self, script_format):
        super(LSFMixin, self).customised_generated_internals(script_format)

        script_format['scheduler_params'] = """\
#BSUB -L /bin/bash
#BSUB -J {job_name}
#BSUB -q queue_name
#BSUB -n {num_procs}
#BSUB -R "affinity[thread(2):cpubind=thread:distribute=pack] span[ptile=80] select[hname==p10a32 || hname==p10a47 || hname==p10a35 || hname==p10a38 || hname==p10a26 || hname==p10a44 || hname==p10a30 || hname==p10a46 || hname=p10a42 || hname==p10a43 || hname==p10a45 || hname==p10a52]"
#BSUB -o {job_output_file}
#BSUB -e {job_error_file}
#BSUB -H\
""".format(**script_format)
        script_format['env_setup'] = """\
export LD_LIBRARY_PATH={coordinator_library_path}:${{LD_LIBRARY_PATH}}

# Export an EC simulation ID (to assist identifying the job in darshan logs)
export EC_simulation_id="{job_name}"\
""".format(**script_format)
        script_format['launch_command'] = """\
{launcher_command} -x LD_LIBRARY_PATH="${{LD_LIBRARY_PATH}}" -N {procs_per_node} -n {num_procs}\
""".format(**script_format)


class Job(LSFMixin, SyntheticAppJob):
    pass

