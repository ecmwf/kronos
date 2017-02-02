import math
import os
import stat

from kronos_executor.job_classes.base_job import BaseJob


class HPCJob(BaseJob):

    submit_script_template = None
    ipm_template = None
    allinea_template = None
    submit_command = None
    launcher_command = None
    allinea_launcher_command = None

    cancel_file = None
    cancel_file_head = None
    cancel_file_line = None

    def __init__(self, job_config, executor, path):
        super(HPCJob, self).__init__(job_config, executor, path)

        self.submit_script = os.path.join(self.path, "submit_script")

    def generate_internal(self):

        nprocs = self.job_config.get('num_procs', 1)
        nnodes = int(math.ceil(float(nprocs) / self.executor.procs_per_node))

        script_format = {
            'write_dir': self.path,
            'read_dir': self.executor.read_cache_path,
            'coordinator_binary': self.executor.coordinator_binary,
            'queue': 'np',
            'num_procs': nprocs,
            'num_nodes': nnodes,
            'num_hyperthreads': 1,
            'input_file': self.input_file,
            'job_dir': self.path,
            'profiling_code': "",
            'experiment_id': 'synthApp_{}_{}_{}'.format(nprocs, nnodes, self.jobno),
            'job_num': self.jobno,
            'job_output_file': os.path.join(self.path, "output"),
            'job_error_file': os.path.join(self.path, "error"),
            'launcher_command': self.launcher_command
        }

        # Enable IPM logging if desired
        if self.executor.enable_ipm:
            script_format['profiling_code'] += self.ipm_template.format(**script_format)

        # Enable Allinea map if desired
        if self.executor.allinea_path is not None and self.executor.allinea_ld_library_path is not None:
            script_format['allinea_path'] = self.executor.allinea_path
            script_format['allinea_ld_library_path'] = self.executor.allinea_ld_library_path
            script_format['launcher_command'] = self.allinea_launcher_command
            
            # append allinea licence file if specified..
            if self.executor.allinea_licence_file:
                script_format['allinea_licence_file'] = self.executor.allinea_licence_file
                self.allinea_template += self.allinea_lic_file_template
            script_format['profiling_code'] += self.allinea_template.format(**script_format)

        with open(self.submit_script, 'w') as f:
            f.write(self.submit_script_template.format(**script_format))

        os.chmod(self.submit_script, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

    def submission_callback(self, output):
        """
        This is the callback function
        """
        if HPCJob.cancel_file is None:
            cancel_file_path = os.path.join(self.executor.job_dir, "killjobs")
            HPCJob.cancel_file = open(cancel_file_path, 'w')
            HPCJob.cancel_file.write(self.cancel_file_head)
            os.chmod(cancel_file_path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IXGRP)

        HPCJob.cancel_file.write(self.cancel_file_line.format(sequence_id=output.strip()))
        HPCJob.cancel_file.flush()

    def run(self):
        """
        'Run' this job

            This has a flexible meaning, depending on the setup. There can be many strategies here.

            i) Add to list to run
            ii) Run it immediately
            iii) Set an async timer to run it in the future?
        """

        self.executor.wait_until(self.start_delay)

        print "Submitting job {}".format(self.jobno)
        self.executor.thread_manager.subprocess_callback(self.submission_callback, self.submit_command, self.submit_script)
