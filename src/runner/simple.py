import os
import subprocess
import time
import glob

import run_control
from base_runner import BaseRunner
from kronos_tools import print_colour
from exceptions_iows import ConfigurationError


class SimpleRunner(BaseRunner):
    """
    Simple Runner class:
        -- runs the model once
    """

    def __init__(self, config):

        # Runner-specific configuration needed
        self.type = None
        self.state = None
        self.tag = None
        self.hpc_user = None
        self.hpc_host = None

        self.hpc_dir_input = None
        self.hpc_dir_output = None
        self.local_map2json_file = None

        # Then set the general configuration into the parent class..
        super(SimpleRunner, self).__init__(config)

    def check_config(self):

        # check simple-runner configuration and pull user options..
        for k, v in self.config.runner.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected simple-runner keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def run(self):

        """
        Run the model on the HPC host according to the configuration options
        output files are left
        :return:
        None
        """

        if self.config.runner['state'] == "enabled":

            job_runner = run_control.factory(self.config.controls['hpc_job_sched'], self.config)

            # rewrite user+host for convenience
            user_at_host = self.hpc_user + '@' + self.hpc_host

            # ------------ init DIR ---------------
            dir_backup = self.config.dir_output+"/"+"backup"
            if not os.path.exists(dir_backup):
                os.makedirs(dir_backup)

            # move the synthetic apps output into kernel dir (and also into bk folder..)
            files = glob.iglob(os.path.join(self.config.dir_output, "*.json"))
            for ff, i_file in enumerate(files):
                if os.path.isfile(i_file):

                    os.system("scp " + i_file + " " + user_at_host + ":" + self.hpc_dir_input)

                    # store output into back-up folder..
                    os.system("cp " + i_file + " " + dir_backup + "/job_sa-" + str(ff) + ".json")

            # run jobs and wait until they have all finished..
            job_runner.remote_run_executor()
            job_runner.have_jobs_finished()

            # search for ".map" files in the output folder
            sub_hdl = subprocess.Popen(["ssh", user_at_host, "find", self.hpc_dir_output, "-name", "*.map"],
                                       shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            sub_hdl.wait()
            list_map_files = sub_hdl.stdout.readlines()
            for (ff, file_name) in enumerate(list_map_files):
                file_name_ok = file_name.replace("\n", "")
                subprocess.Popen(["scp", user_at_host+":"+file_name_ok, self.config.dir_input+"/"+"job-"+str(ff)+".map"]).wait()

                time.sleep(2.0)
                subprocess.Popen(["python", self.local_map2json_file, self.config.dir_input+"/"+"job-"+str(ff)+".map"]).wait()

            # once all the files have been copied over to local input, rename the hpc output folder
            output_dst = self.hpc_dir_output+"_iter_0" if self.hpc_dir_output[-1] is not'/' else self.hpc_dir_output[:-1]+"_iter_0"
            subprocess.Popen(["ssh", user_at_host, "mv", self.hpc_dir_output, output_dst]).wait()
            # --------------------------------------------------------------------------

        else:

            print_colour.print_colour("orange", "runner NOT enabled. Model did not run!")

    def plot_results(self):

        print_colour.print_colour("orange", "plotting not yet implemented..")
