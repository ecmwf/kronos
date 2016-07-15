#!/usr/bin/env python

# Add parent directory as search path form modules
import os
import sys
import shutil
import subprocess
import time
import re

import glob

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from real_workload import RealWorkload
from plot_handler import PlotHandler


def feedback_loop():

    # Load config
    config = Config()
    plot_tag = "test_SA_"

    # create dummy workload by synthetic apps
    run_dir = "/var/tmp/maab/iows/input"

    # Initialise the input workload
    scheduler_tag = ""
    scheduler_log_file = ""
    profiler_tag = "allinea"
    profiler_log_dir = "/var/tmp/maab/iows/input"
    input_workload = RealWorkload(config)
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)
    input_workload.make_plots(plot_tag)

    # Generator model
    scaling_factor = 100  # [%] percentage of measured workload
    model = IOWSModel(config)
    model.set_input_workload(input_workload)
    model.create_scaled_workload("time_plane", "Kmeans", scaling_factor)
    model.export_scaled_workload()
    model.make_plots(plot_tag)

    # -------- Run the synthetic apps and profile them with Allinea ------------
    n_modelled_jobs = len(model.syntapp_list)
    iows_dir = "/var/tmp/maab/iows"
    synth_app_run_dir = "/perm/ma/maab/kronos_run"
    kronos_run_dir = "/perm/ma/maab/kronos_run"
    map2json_dir_bin = "/home/ma/maab/workspace/Allinea_examples_files/NEXTGenIO_buiild/map2json"

    # move the synthetic apps output into kernel dir
    files = glob.iglob(os.path.join(iows_dir+"/output", "*.json"))
    for i_file in files:
        if os.path.isfile(i_file):
            print "scp "+i_file+" "+kronos_run_dir+"/input"
            os.system("scp "+i_file+" maab@ccb:"+kronos_run_dir+"/input")

    # run the executor
    # ret = subprocess.call(["ssh", "maab@ccb", synth_app_run_dir+"/input/run_jobs"])

    # ..and wait until it completes ----------
    jobs_completed = False
    while not jobs_completed:
        ssh_ls_cmd = subprocess.Popen(["ssh", "maab@ccb", "find " + kronos_run_dir + "/output -name *.map"],
                                      shell=False,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)

        if len(ssh_ls_cmd.stdout.readlines()) == n_modelled_jobs:
            jobs_completed = True

        print "jobs completed? : " + str(jobs_completed)
        time.sleep(1.0)
    # ----------------------------------------

    # once all the jobs have completed, copy the map files back over..
    ssh_ls_cmd = subprocess.Popen(["ssh", "maab@ccb", "find "+kronos_run_dir+"/output -name *.map"],
                                  shell=False,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)

    for (ff, file_name) in enumerate(ssh_ls_cmd.stdout.readlines()):
        file_name_ok = file_name.replace("\n","")
        subprocess.Popen(["scp", "maab@ccb:"+file_name_ok, iows_dir+"/input/"+"job-"+str(ff)+".map"])
    # --------------------------------------------------------------------------

    # scheduler_tag = ""
    # scheduler_log_file = ""
    # profiler_tag = "allinea"
    # profiler_log_dir = "/home/ma/maab/workspace/iows/input/run_sa/"
    # sa_workload = RealWorkload(config)
    # sa_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)

    # Create plots by comparing the logs..

    # check num plots
    PlotHandler.print_fig_handle_ID()


if __name__ == '__main__':

    feedback_loop()
