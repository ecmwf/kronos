#!/usr/bin/env python

# Add parent directory as search path form modules
import os
import numpy as np

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from model_workload import ModelWorkload
from plot_handler import PlotHandler
import time_signal


def test_synth_app_0():

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
    input_workload = ModelWorkload(config)
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)
    input_workload.make_plots(plot_tag)
    input_workload.print_global_sums()

    # ----------- initialize the vectors: metric_sums, deltas, etc... -----------
    ts_names = time_signal.signal_types.keys()
    scaling_factor_dict = {'kb_collective': 1,
                     'n_collective': 1,
                     'n_pairwise': 1,
                     'kb_write': 1,
                     'kb_read': 0.3,
                     'flops': 0,
                     'kb_pairwise': 1,
                     }
    # ---------------------------------------------------------

    model = IOWSModel(config, input_workload)

    synthetic_apps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_dict, reduce_jobs_flag=False)
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)

    synthetic_apps.print_metrics_sums()

    # check num plots
    PlotHandler.print_fig_handle_ID()


if __name__ == '__main__':

    test_synth_app_0()
