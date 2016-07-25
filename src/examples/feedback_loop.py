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
    PLOT_TAG = "convergence_sa_"

    # create dummy workload by synthetic apps
    SCHEDULER_TAG = ""
    SCHEDULER_LOG_FILE = ""
    PROFILER_TAG = "allinea"
    PROFILER_LOG_DIR = "/var/tmp/maab/iows/input"

    # dir for iows and kronos
    IOWS_DIR = "/var/tmp/maab/iows"
    IOWS_DIR_BACKUP = IOWS_DIR + "/input/" + "last_run_bk"
    KRONOS_RUN_DIR = "/scratch/ma/maab/kronos_run"
    USER_HOST = "maab@ccb"

    # ---- replay options -----
    REPLAY_RUN = False
    REPLAY_RUN_NJOBS = 9
    # -------------------------

    # ---- loop settings.. ----
    n_iterations = 6
    relaxation_factor = 0.9

    # initialize the vectors: metric_sums, deltas, etc...
    # [%] percentage of measured workload (per each metric)
    scaling_factor_list = [100.,
                           100.,
                           100.,
                           # 100.,
                           # 100.,
                           # 100.,
                           # 100.,
                           ]

    metrics_sums_history = np.ndarray([0, len(config.WORKLOADCORRECTOR_LIST_TIME_NAMES)])
    scaling_factors_history_raw = np.zeros(metrics_sums_history.shape)
    scaling_factors_history_raw = np.vstack([scaling_factors_history_raw, np.asarray(scaling_factor_list)])
    scaling_factors_history_raw = np.vstack([scaling_factors_history_raw, np.asarray(scaling_factor_list)])
    scaling_factors_history_raw = np.vstack([scaling_factors_history_raw, np.asarray(scaling_factor_list)])

    # run jobs on the cluster
    if not REPLAY_RUN:

        # Initialise the input workload
        iter_workload = RealWorkload(config)
        iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR)
        iter_workload.make_plots(PLOT_TAG)
        reference_metrics_iter0 = np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])

        print "reference_metrics_iter0 (iter=0): ", reference_metrics_iter0

        # main loop: iterates to minimize the difference with the initial WL
        for i_count in range(0, n_iterations):

            # Generator model
            model = IOWSModel(config)
            model.set_input_workload(iter_workload)
            model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_list, reduce_jobs_flag=False)
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
                    print "jobs completed!"

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
                proc_scp = subprocess.Popen(["scp", USER_HOST+":"+file_name_ok, IOWS_DIR+"/input/"+"job-"+str(ff)+".map"])
                proc_scp.wait()

                time.sleep(1.0)
                proc_m2j = subprocess.Popen(["python", IOWS_DIR+"/input/map2json.py", IOWS_DIR+"/input/"+"job-"+str(ff)+".map"])
                proc_m2j.wait()

            # once all the files have been copied over to local input, rename the host run folder "jobs"
            subprocess.Popen(["ssh",
                              USER_HOST,
                              "mv "+KRONOS_RUN_DIR+"/output/jobs "+KRONOS_RUN_DIR+"/output/jobs_iter_"+str(i_count)]),
            # --------------------------------------------------------------------------

            # re-calculate the workload workload
            time.sleep(5.0)

            # set list of json for this iteration
            list_json_files = ["job-"+str(ff)+"."+str(i_count+1)+".json" for (ff, file_name) in enumerate(list_map_files)]

            iter_workload = RealWorkload(config)
            iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
            iter_workload.make_plots(PLOT_TAG)
            metrics_sums_history = np.vstack([metrics_sums_history,
                                      np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])]
                                    )

            # re-calculate scaling factor according to measured deltas..
            sc_factors_vec_raw = (np.asarray(metrics_sums_history[0, :]))/np.asarray(metrics_sums_history[-1, :]) * 100.

            sc_factors_vec = relaxation_factor * sc_factors_vec_raw + \
                             (1.-relaxation_factor)/2. * scaling_factors_history_raw[-1, :] + \
                             (1.-relaxation_factor)/2. * scaling_factors_history_raw[-2, :]

            scaling_factors_history_raw = np.vstack([scaling_factors_history_raw, sc_factors_vec_raw])
            scaling_factor_list = sc_factors_vec.tolist()

            print "metrics_sums_history[-1,:]: ", metrics_sums_history[-1, :]
            print "scaling_factors_history_raw[-1,:]: ", scaling_factors_history_raw[-1, :]
            print "scaling_factor_list: ", scaling_factor_list

    else:

        print "Replaying run.."

        # set list of json for this iteration
        list_json_files = ["job-" + str(ff) + ".json" for ff in range(0, REPLAY_RUN_NJOBS)]
        iter_workload = RealWorkload(config)
        iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
        reference_metrics_iter0 = np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])

        for i_count in range(0, n_iterations):

            # set list of json for this iteration
            list_json_files = ["job-"+str(ff)+"."+str(i_count+1)+".json" for ff in range(0, REPLAY_RUN_NJOBS)]

            # Initialise the input workload
            iter_workload = RealWorkload(config)
            iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
            metrics_sums_history = np.vstack([metrics_sums_history, np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])])

            print "metrics_sums: ", metrics_sums_history[-1, :]

            # re-calculate scaling factor according to measured deltas..
            sc_factors_vec_raw = (np.asarray(metrics_sums_history[0, :])) / np.asarray(
                metrics_sums_history[-1, :]) * 100.

            sc_factors_vec = relaxation_factor * sc_factors_vec_raw + \
                             (1. - relaxation_factor) / 2. * scaling_factors_history_raw[-1, :] + \
                             (1. - relaxation_factor) / 2. * scaling_factors_history_raw[-2, :]

            print "sc_factors_vec: ", sc_factors_vec

            scaling_factors_history_raw = np.vstack([scaling_factors_history_raw, sc_factors_vec_raw])


    # ------------- make convergence plot -------------
    n_iter = metrics_sums_history.shape[0]
    n_metrics = metrics_sums_history.shape[1]
    metrics_names = [i_ts[0] for i_ts in config.WORKLOADCORRECTOR_LIST_TIME_NAMES]

    plt.figure(999, figsize=(8, 12), dpi=80, facecolor='w', edgecolor='k')
    for imetr in range(0, n_metrics):
        plt.subplot(n_metrics, 1, imetr+1)

        ref_vals = reference_metrics_iter0[imetr]*np.ones((n_iter))
        iter_vals = metrics_sums_history[:, imetr]

        plt.plot(np.asarray(range(0, n_iter)), ref_vals,'b')
        plt.plot(np.asarray(range(0, n_iter)), iter_vals, 'r-*')

        plt.legend(['ref', 'simulated'])
        plt.ylabel(metrics_names[imetr])
        plt.xticks(range(0, n_iter+1))
        plt.xlim(xmin=0, xmax=n_iter+1.1)
        plt.ylim(ymin=0, ymax=max( np.hstack((iter_vals, ref_vals)))*1.1)

    plt.savefig(config.DIR_OUTPUT + "/" + PLOT_TAG + "_plot_iterations.png")
    # ------------------------------------

    # ------------------------------- write log file -------------------------------------
    if not os.path.exists(IOWS_DIR_BACKUP):
        os.makedirs(IOWS_DIR_BACKUP)
    np.savetxt(IOWS_DIR_BACKUP + '/metrics_sums.txt', metrics_sums_history, delimiter=",")
    np.savetxt(IOWS_DIR_BACKUP + '/scaling_factors_raw.txt', scaling_factors_history_raw, delimiter=",")
    # ------------------------------------------------------------------------------------

    # check num plots
    PlotHandler.print_fig_handle_ID()

if __name__ == '__main__':

    feedback_loop()
