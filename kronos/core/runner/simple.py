# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import subprocess
import time
import datetime

from kronos.core import run_control
from kronos.core.runner.base_runner import BaseRunner
from kronos.core.kronos_tools import print_colour
from kronos.core.exceptions_iows import ConfigurationError


class SimpleRunner(BaseRunner):
    """
    Simple Runner class:
        -- runs the model once
    """

    def __init__(self, config):

        # Runner-specific configuration needed
        self.config = config

        self.type = None
        self.state = None
        self.hpc_user = None
        self.hpc_host = None
        self.tag = None

        self.hpc_dir_input = None
        self.hpc_dir_output = None
        self.local_map2json_file = None

        self.ksf_filename = self.config.ksf_filename

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

            user_at_host = self.hpc_user + '@' + self.hpc_host
            time_now_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d_%H-%M-%S')
            dir_run_results = os.path.join(self.config.dir_output, 'fl_run_{}'.format(time_now_str))

            job_runner = run_control.factory(self.config.controls['hpc_job_sched'], self.config)

            # # handles the ksf file (not needed to read it here..)
            # ksf_data = KSFFileHandler().from_ksf_file(os.path.join(self.config.dir_output, self.ksf_filename))

            # create run dir
            if not os.path.exists(dir_run_results):
                os.makedirs(dir_run_results)

            # -- SA jsons iteration folder
            dir_run_iter_sa = os.path.join(dir_run_results, 'sa_jsons')
            if not os.path.exists(dir_run_iter_sa):
                os.makedirs(dir_run_iter_sa)

            # -- MAP jsons iteration folder
            dir_run_iter_map = os.path.join(dir_run_results, 'run_jsons')
            if not os.path.exists(dir_run_iter_map):
                os.makedirs(dir_run_iter_map)

            # move the ksf file into HPC input dir (and also into SA iteration folder)
            subprocess.Popen(["scp",
                              os.path.join(self.config.dir_output, self.ksf_filename),
                              user_at_host + ":" + self.hpc_dir_input])
            subprocess.Popen(["cp",
                              os.path.join(self.config.dir_output, self.ksf_filename),
                              os.path.join(dir_run_results, self.ksf_filename)])

            # run jobs and wait until they have all finished..
            job_runner.remote_run_executor()
            job_runner.have_jobs_finished()

            # search for ".map" files in the output folder
            sub_hdl = subprocess.Popen(["ssh", user_at_host, "find", self.hpc_dir_output, "-name", "*.map"],
                                       shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            sub_hdl.wait()

            # ------ fetch the output map files and copy them into the MAP iteration folder ------
            list_map_files = sub_hdl.stdout.readlines()
            for (ff, file_name) in enumerate(list_map_files):
                file_name_ok = file_name.replace("\n", "")
                subprocess.Popen(["scp",
                                  user_at_host+":"+file_name_ok,
                                  os.path.join(dir_run_iter_map, "job-"+str(ff)+".map")
                                  ]).wait()
                time.sleep(2.0)
                subprocess.Popen(["python",
                                  self.local_map2json_file,
                                  os.path.join(dir_run_iter_map, "job-"+str(ff)+".map")
                                  ]).wait()
            # -----------------------------------------------------------------------------------

            # -------------------- finally rename the HPC output folder -------------------------
            output_dst = self.hpc_dir_output.rstrip('/')+"_iter_0"
            subprocess.Popen(["ssh", user_at_host, "mv", self.hpc_dir_output, output_dst]).wait()
            # -----------------------------------------------------------------------------------

        else:

            print_colour.print_colour("orange", "runner NOT enabled. Model did not run!")

    def plot_results(self):

        print_colour.print_colour("orange", "plotting not yet implemented..")