#!/usr/bin/env python

# Add parent directory as search path form modules
import os
import sys
import shutil
import subprocess
import time
import re
import numpy as np
import matplotlib.pyplot as plt

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
    PLOT_TAG = "test_SA_"

    # create dummy workload by synthetic apps
    SCHEDULER_TAG = ""
    SCHEDULER_LOG_FILE = ""
    PROFILER_TAG = "allinea"
    PROFILER_LOG_DIR = "/var/tmp/maab/iows/input"

    # dir for iows and kronos
    IOWS_DIR = "/var/tmp/maab/iows"
    KRONOS_RUN_DIR = "/scratch/ma/maab/kronos_run"
    USER_HOST = "maab@ccb"

    # ---- loop settings.. ----
    n_iterations = 5
    relaxation_factor = 0.1

    # initialize the vectors: metric_sums, deltas, etc...
    metrics_sums_history = np.ndarray([0,len(config.WORKLOADCORRECTOR_LIST_TIME_NAMES)])
    deltas_vec_history = np.zeros(metrics_sums_history.shape)

    # [%] percentage of measured workload (per each metric)
    scaling_factor_list = [100.,
                           100.,
                           100.,
                           # 100.,
                           # 100.,
                           # 100.,
                           # 100.,
                           ]

    # Initialise the input workload
    iter_workload = RealWorkload(config)
    iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR)
    iter_workload.make_plots(PLOT_TAG)
    metrics_sums_history = np.vstack( [metrics_sums_history, np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])] )

    print "metrics_sums_history (iter=0): ", metrics_sums_history[-1, :]

    # main loop: iterates to minimize the difference with the initial WL
    for i_count in range(0, n_iterations):

        # print "scaling_factor_list ", scaling_factor_list

        # Generator model
        model = IOWSModel(config)
        model.set_input_workload(iter_workload)
        model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_list)
        model.export_scaled_workload()
        model.make_plots(PLOT_TAG)

        # -------- Run the synthetic apps and profile them with Allinea ------------
        n_modelled_jobs = len(model.syntapp_list)

        # move the synthetic apps output into kernel dir
        files = glob.iglob(os.path.join(IOWS_DIR+"/output", "*.json"))
        for i_file in files:
            if os.path.isfile(i_file):
                # print "scp "+i_file+" "+KRONOS_RUN_DIR+"/input"
                os.system("scp "+i_file+" "+USER_HOST+":"+KRONOS_RUN_DIR+"/input")

        # run the executor
        ret = subprocess.call(["ssh", USER_HOST, KRONOS_RUN_DIR+"/input/run_jobs"])
        time.sleep(2.0)

        # ..and wait until it completes ----------
        jobs_completed = False
        while not jobs_completed:
            ssh_ls_cmd = subprocess.Popen(["ssh", USER_HOST, "qstat -u maab"],
                                          shell=False,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
            jobs_in_queue = ssh_ls_cmd.stdout.readlines()

            if jobs_in_queue == []:
                jobs_completed = True
                print "jobs_in_queue:", jobs_in_queue

            time.sleep(2.0)
        # ----------------------------------------

        # ----------------------------------------
        time.sleep(10.0)
        ssh_ls_cmd = subprocess.Popen(["ssh", USER_HOST, "find " + KRONOS_RUN_DIR + "/output/jobs -name *.map"],
                                      shell=False,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)

        list_map_files = ssh_ls_cmd.stdout.readlines()
        for (ff, file_name) in enumerate(list_map_files):
            file_name_ok = file_name.replace("\n","")
            subprocess.Popen(["scp", USER_HOST+":"+file_name_ok, IOWS_DIR+"/input/"+"job-"+str(ff)+".map"])

            # TODO: proper check that all the files have been copied over...
            time.sleep(3.0)
            subprocess.call(["python", IOWS_DIR+"/input/map2json.py", IOWS_DIR+"/input/"+"job-"+str(ff)+".map"])

        # once all the files have been copied over to local input, rename the host run folder "jobs"
        subprocess.Popen(["ssh",
                          USER_HOST,
                          "mv "+KRONOS_RUN_DIR+"/output/jobs "+KRONOS_RUN_DIR+"/output/jobs_iter_"+str(i_count)]),
        # --------------------------------------------------------------------------

        # re-calculate the workload workload
        time.sleep(5.0)

        # set list of json for this iteration
        list_json_files = ["job-"+str(ff)+"."+str(i_count+1)+".json" for (ff, file_name) in enumerate(list_map_files)]

        print list_json_files

        iter_workload = RealWorkload(config)
        iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
        iter_workload.make_plots(PLOT_TAG)
        metrics_sums_history = np.vstack([metrics_sums_history,
                                  np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])]
                                )

        # re-calculate scaling factor according to measured deltas..
        deltas_vec = (metrics_sums_history[-1, :] - metrics_sums_history[-2, :])/metrics_sums_history[-2, :]
        scaling_factor_vec_norm = np.asarray(scaling_factor_list)/100
        deltas_vec_relax_norm = np.clip(deltas_vec*relaxation_factor, -2.0, 2.0)

        # print "scaling_factor_vec_norm: ", scaling_factor_vec_norm
        # print "deltas_vec_relax_norm: ", deltas_vec_relax_norm
        print "metrics_sums_history[-1,:]: ", metrics_sums_history[-1, :]

        scaling_factor_list = (scaling_factor_vec_norm - deltas_vec_relax_norm)*100
        scaling_factor_list = scaling_factor_list.tolist()
        deltas_vec_history = np.vstack([deltas_vec_history, deltas_vec])

        # restore scaling factor list.. TODO: TO BE REMOVED!!
        scaling_factor_list = [100.,
                               100.,
                               100.,
                               # 100.,
                               # 100.,
                               # 100.,
                               # 100.,
                               ]

    # -------------------------------

    # ------------ make plot -------------
    n_iter = metrics_sums_history.shape[0]
    n_metrics = metrics_sums_history.shape[1]
    metrics_names = [i_ts[0] for i_ts in config.WORKLOADCORRECTOR_LIST_TIME_NAMES]

    plt.figure(999, figsize=(8, 12), dpi=80, facecolor='w', edgecolor='k')
    for imetr in range(0, n_metrics):
        plt.subplot(n_metrics, 1, imetr+1)

        ref_vals = metrics_sums_history[0,imetr]*np.ones((n_iter,1))
        iter_vals = metrics_sums_history[:, imetr]

        plt.plot(np.asarray(range(0, n_iter)), ref_vals,'b')
        plt.plot(np.asarray(range(0, n_iter)), iter_vals, 'r-*')

        plt.legend(['ref', 'simulated'])
        plt.ylabel(metrics_names[imetr])
        plt.xticks(range(0, n_iter+1))
        plt.xlim(xmin=0, xmax=n_metrics+1.1)
        plt.ylim(ymin=0, ymax=max(iter_vals)*2.0)

    plt.savefig(config.DIR_OUTPUT + '/plot_iterations.png')
    # ------------------------------------

    # check num plots
    PlotHandler.print_fig_handle_ID()

if __name__ == '__main__':

    feedback_loop()
