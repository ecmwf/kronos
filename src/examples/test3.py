#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from config.config import Config
from IOWS_model import IOWSModel
from model_workload import ModelWorkload
from plot_handler import PlotHandler


def test3():

    # Load config
    config = Config()
    plot_tag = "test_NEW_"

    scheduler_tag = "pbs"
    # scheduler_log_file = "/perm/ma/maab/PBS_log_example/20151123_test_2k"
    scheduler_log_file = "/perm/ma/maab/PBS_log_example/20151123_test_10"


    profiler_tag = "allinea"
    profiler_log_dir = "/home/ma/maab/workspace/Allinea_examples_files/my_tests/cca_IOR_map_NG_NEWBUILD"

    # Initialise the input workload
    input_workload = ModelWorkload(config)
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)
    input_workload.make_plots(plot_tag)

    # Generator model
    # TODO: Can IOWSModel functionality be incorporated into the ModelWorkload class as an output?
    scaling_factor = 100  # [%] percentage of measured workload
    model = IOWSModel(config, input_workload)
    synthetic_apps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factor)

    # And do the output!
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)


    model.make_plots(plot_tag)

    # check num plots
    PlotHandler.print_fig_handle_ID()

if __name__ == '__main__':

    test3()
