#!/usr/bin/env python

# Add parent directory as search path form modules
import os
import subprocess
import time
import argparse
import glob
import numpy as np
import matplotlib.pyplot as plt


os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from model_workload import ModelWorkload
from plot_handler import PlotHandler
import time_signal


# ////////////////////////// config parameters ////////////////////////////

RUN_TAG = "run_" + str(time.time())

# create dummy workload by synthetic apps
SCHEDULER_TAG = ""
SCHEDULER_LOG_FILE = ""
PROFILER_TAG = "allinea"
PROFILER_LOG_DIR = "/var/tmp/maab/iows/input"

# dir for iows and kronos
IOWS_DIR = "/var/tmp/maab/iows"
IOWS_DIR_INPUT = IOWS_DIR + "/input"
IOWS_DIR_OUTPUT = IOWS_DIR + "/output"
IOWS_DIR_BACKUP = IOWS_DIR_INPUT + "/" + RUN_TAG

KRONOS_RUN_DIR = "/scratch/ma/maab/kronos_run"
USER_HOST = "maab@ccb"

n_iterations = 2

LOG_FILE = IOWS_DIR_BACKUP + '/' + RUN_TAG + '_log.txt'
ts_names = time_signal.signal_types.keys()
updatable_metrics = {'kb_collective':1,
                     'n_collective':1,
                     'n_pairwise':1,
                     'kb_write':1,
                     'kb_read':1,
                     'flops':0,
                     'kb_pairwise':1,
                    }
# /////////////////////////////////////////////////////////////////////////


def run():

    # Load config
    config = Config()

    # ------------ init DIR ---------------
    if not os.path.exists(IOWS_DIR_BACKUP):
        os.makedirs(IOWS_DIR_BACKUP)

    # ----------- initialize the vectors: metric_sums, deltas, etc... -----------
    scaling_factor_dict = {}
    for m in ts_names:
        scaling_factor_dict[m] = 1.0
    # ---------------------------------------------------------

    # ----------------- init log file --------------------
    with open(LOG_FILE, "w") as myfile:
        ts_names_str = ''.join(e + ' ' for e in ts_names)
        myfile.write(ts_names_str + '\n')
    # ----------------------------------------------------

    metrics_sums_history = []
    scaling_factors_history = []

    # Initialise the input workload
    iter_workload = ModelWorkload(config)
    iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR)
    iter_workload.make_plots(RUN_TAG)

    # Generator model
    model = IOWSModel(config, iter_workload)
    synthetic_apps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_dict, reduce_jobs_flag=True)
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)

    # store reference metrics sums (they are later used to calculate scaling factors..)
    metrics_sum_dict_ref = iter_workload.total_metrics_sum_dict()
    sa_metric_dict = synthetic_apps.total_metrics_dict

    print "ts names          : ", ts_names
    print "reference metrics : ", mytools.sort_dict_list(metrics_sum_dict_ref, ts_names)
    print "sa tot metrics    : ", mytools.sort_dict_list(sa_metric_dict, ts_names)

    # write log file
    write_log_file(metrics_sum_dict_ref, scaling_factor_dict)
    # ---------------------------------------------------------------------------

    # store input jsons into back-up folder..
    iter0_json_files = [pos_json for pos_json in os.listdir(IOWS_DIR_INPUT) if pos_json.endswith('.json')]
    for i_file in iter0_json_files:
        print "back-up of file: ", i_file
        os.system("cp " + IOWS_DIR_INPUT + "/" + i_file + " " + IOWS_DIR_BACKUP)


    # main loop..
    for i_count in range(0, n_iterations):

        print "=======================================> ITERATION: ", i_count

        # -------- Run the synthetic apps and profile them with Allinea ------------
        # move the synthetic apps output into kernel dir (and also into bk folder..)
        files = glob.iglob(os.path.join(IOWS_DIR_OUTPUT, "*.json"))
        for ff, i_file in enumerate(files):
            if os.path.isfile(i_file):
                # print "scp "+i_file+" "+KRONOS_RUN_DIR+"/input"
                os.system("scp " + i_file + " " + USER_HOST + ":" + KRONOS_RUN_DIR + "/input")

                # store output into back-up folder..
                os.system("cp " + i_file + " " + IOWS_DIR_BACKUP + "/job_sa-" + str(ff) + "_iter-" + str(
                    i_count + 1) + ".json")

        # ------------------------- run the executor --------------------------------
        ret = subprocess.call(["ssh", USER_HOST, KRONOS_RUN_DIR + "/input/run_jobs"])
        time.sleep(2.0)
        # ---------------------------------------------------------------------------

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
            file_name_ok = file_name.replace("\n", "")
            proc_scp = subprocess.Popen(
                ["scp", USER_HOST + ":" + file_name_ok, IOWS_DIR_INPUT + "/" + "job-" + str(ff) + ".map"])
            proc_scp.wait()

            time.sleep(2.0)
            proc_m2j = subprocess.Popen(
                ["python", IOWS_DIR_INPUT + "/map2json.py", IOWS_DIR_INPUT + "/" + "job-" + str(ff) + ".map"])
            proc_m2j.wait()

        # once all the files have been copied over to local input, rename the host run folder "jobs"
        subprocess.Popen(["ssh",
                          USER_HOST,
                          "mv " + KRONOS_RUN_DIR + "/output/jobs " + KRONOS_RUN_DIR + "/output/jobs_iter_" + str(i_count)]),
        # --------------------------------------------------------------------------

        # re-calculate the workload workload
        time.sleep(5.0)

        # set list of json for this iteration
        list_json_files = [IOWS_DIR_INPUT + "/" + "job-" + str(ff) + "." + str(i_count + 1) + ".json" for
                           (ff, file_name) in enumerate(list_map_files)]

        # store input jsons into back-up folder..
        for i_file in list_json_files:
            os.system("cp " + i_file + " " + IOWS_DIR_BACKUP)

        # re-read logs..
        iter_workload = ModelWorkload(config)
        iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
        metrics_sum_dict = iter_workload.total_metrics_sum_dict()

        # append dictionaries to history structures..
        metrics_sums_history.append(metrics_sum_dict)
        scaling_factors_history.append(scaling_factor_dict)

        # calculate current scaling factor..
        scaling_factor_dict = get_new_scaling_factors(metrics_sum_dict_ref, metrics_sums_history, scaling_factors_history)

        # write log file
        write_log_file(metrics_sum_dict, scaling_factor_dict)

        # Re Generate the whole model from ORIGINAL JSONS but with the UPDATED scaling factors..
        list_json_files = [IOWS_DIR_INPUT + "/" + "job-" + str(ff) + ".json" for (ff, file_name) in
                           enumerate(list_map_files)]
        iter_workload = ModelWorkload(config)
        iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)

        model = IOWSModel(config, iter_workload)
        synthetic_apps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_dict,
                                                      reduce_jobs_flag=False)
        synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)


def plot_run(logfile_name=None):

    assert logfile_name is not None

    # ------------- make convergence plot -------------
    with open(logfile_name, "r") as logfile_name:
        content = logfile_name.readlines()
    content = [x.strip('\n') for x in content]

    # exclude the titles..
    content = content[1:]

    # cast the content
    content = [map(float, row.split()) for row in content]

    # reference metrics (from iteration 0)
    ref_metrics = content[0]

    # remove the ref values from content..
    content = content[1:]
    n_iter = len(content)
    n_metrics = len(ts_names)

    plt.figure(999, figsize=(8, 12), dpi=80, facecolor='w', edgecolor='k')

    for mm, metric in enumerate(ts_names):
        plt.subplot(n_metrics, 1, mm + 1)

        ref_vals = ref_metrics[mm] * np.ones(n_iter)
        iter_vals = np.asarray([row[mm] for row in content])

        xx = np.asarray(range(0, n_iter))
        plt.plot(xx, ref_vals, 'b')
        plt.plot(xx, iter_vals, 'r-*')

        plt.legend(['ref', 'simulated'])
        plt.ylabel(metric)
        plt.xticks(range(0, n_iter + 1))
        plt.xlim(xmin=0, xmax=n_iter + 1.1)

    plt.savefig(IOWS_DIR_OUTPUT + "/" "_plot_iterations.png")
    # -------------------------------------------------

    # check num plots
    PlotHandler.print_fig_handle_ID()


def replay_run():

    pass

    # print "Replaying run.."
    #
    # # ---- replay options -----
    # REPLAY_RUN = False
    # if REPLAY_RUN:
    #     REPLAY_DIR = ""
    #     REPLAY_RUN_NJOBS = 9
    #     RUN_TAG = "replay_"
    # # -------------------------
    #
    #
    # scaling_factor_list = []
    # for m in ordered_list_metrics:
    #     scaling_factor_list.append(scaling_factor_dict[m])
    #
    # # set list of json for this iteration
    # list_json_files = [REPLAY_DIR + "/" + "job-" + str(ff) + ".json" for ff in range(0, REPLAY_RUN_NJOBS)]
    # iter_workload = ModelWorkload(config)
    # iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
    #
    # metrics_sums_history = np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])
    # reference_metrics = metrics_sums_history
    # scaling_factors_history = np.zeros((0, len(metrics_sums_history)))
    #
    # print "metrics_sums_history: ", metrics_sums_history
    # print "reference_metrics: ", reference_metrics
    # print "scaling_factors_history: ", scaling_factors_history
    # # ---------------------------------------------------------------------------
    #
    # for i_count in range(0, n_iterations):
    #
    #     # set list of json for this iteration
    #     list_json_files = [REPLAY_DIR + "/" + "job-"+str(ff)+"."+str(i_count+1)+".json" for ff in range(0, REPLAY_RUN_NJOBS)]
    #
    #     # Initialise the input workload
    #     iter_workload = ModelWorkload(config)
    #     iter_workload.read_logs(SCHEDULER_TAG, PROFILER_TAG, SCHEDULER_LOG_FILE, PROFILER_LOG_DIR, list_json_files)
    #
    #     metrics_sum_iter = np.asarray([i_sum.sum for i_sum in iter_workload.total_metrics])
    #     metrics_sums_history = np.vstack([metrics_sums_history, metrics_sum_iter])
    #
    #     # re-calculate scaling factor according to measured deltas..
    #     sc_factors_vec = (np.asarray(reference_metrics)) / np.asarray(metrics_sums_history[-1, :])
    #     scaling_factors_history = np.vstack([scaling_factors_history, sc_factors_vec])
    #
    #     # multiply old scaling factors by new scaling factors...
    #     scaling_factor_list = [o * n for (o, n) in zip(scaling_factor_list, sc_factors_vec.tolist())]
    #
    #     print "----------------------- iter = " + str(i_count) + " ---------------------------"
    #     print "metrics REF: ", reference_metrics
    #     print "metrics_sums: ", metrics_sums_history[-1, :]
    #     print "sc_factors_vec: ", sc_factors_vec
    #     print "scaling_factor_list: ", scaling_factor_list
    #     print "------------------------------------------------------------"


def get_new_scaling_factors(metrics_ref, metrics_history, scaling_history, key=None):

    """ re-calculate scaling factor according to measured deltas.. """

    scaling_factor_dict_new = {}
    metrics_iter_current = metrics_history[-1]
    # metrics_iter_minus_1 = metrics_history[-2]

    scaling_current = scaling_history[-1]
    # scaling_minus_1 = scaling_history[-2]

    for metric in ts_names:
        if metrics_iter_current[metric] < 1.0e-10 or not updatable_metrics[metric]:
            scaling_factor_dict_new[metric] = 1.0
        else:
            scaling_factor_dict_new[metric] = metrics_ref[metric] / metrics_iter_current[metric] * scaling_current[metric]

    print "----------------------- summary: ---------------------------"
    print "ts names            : ", ts_names
    print "scaling factors     : ", mytools.sort_dict_list(scaling_current, ts_names)
    print "reference metrics   : ", mytools.sort_dict_list(metrics_ref, ts_names)
    print "metrics sums        : ", mytools.sort_dict_list(metrics_iter_current, ts_names)
    print "scaling factors new : ", mytools.sort_dict_list(scaling_factor_dict_new, ts_names)
    print "------------------------------------------------------------"

    return scaling_factor_dict_new


def write_log_file(metrics_sum_dict, scaling_factor_dict):

    # append metrics sums and scaling factors to log file..
    with open(LOG_FILE, "a") as myfile:
        metrics_str = ''.join(str(e) + ' ' for e in mytools.sort_dict_list(metrics_sum_dict, ts_names))
        scaling_str = ''.join(str(e) + ' ' for e in mytools.sort_dict_list(scaling_factor_dict, ts_names))
        myfile.write(metrics_str + scaling_str + '\n')



# /////////////////////////////////////////////////////////////////////////
if __name__ == '__main__':

    # Create a parser for the option passed in
    parser = argparse.ArgumentParser(description="start feed-back loop")
    parser.add_argument("flag", nargs='?', default=None, help="plot flag..")
    parser.add_argument("logfile_name", nargs='?', default=None, help="directory that contains the log file..")
    args = parser.parse_args()

    # Check that the map file that is passed in points to an existing file
    flag = args.flag

    if flag == "run":

        run()

    elif flag == "plot":

        assert args.logfile_name is not None

        plot_run(logfile_name=args.logfile_name)

    elif flag == "replay":

        pass
# /////////////////////////////////////////////////////////////////////////
