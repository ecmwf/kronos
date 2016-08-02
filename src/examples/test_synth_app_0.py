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

    print "input_workload metrics_sums: ", np.asarray([i_sum.sum for i_sum in input_workload.total_metrics])

    # Generator model
    # [%] percentage of measured workload (per each metric)
    # scaling_factor_list = [.35,
    #                        .35,
    #                        .35,
    #                        # 1.,
    #                        # 1.,
    #                        # 1.,
    #                        # 1.,
    #                        ]

    scaling_factor_list = [1.,
                           1.,
                           1.,
                           # 1.,
                           # 1.,
                           # 1.,
                           # 1.,
                           ]

    model = IOWSModel(config, input_workload)

    synthetic_apps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factor_list, reduce_jobs_flag=True)
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)

    print "synthetic apps time signals sums: ", synthetic_apps.total_metrics

    # check num plots
    PlotHandler.print_fig_handle_ID()


if __name__ == '__main__':

    test_synth_app_0()
