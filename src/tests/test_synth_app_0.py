#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from real_workload import RealWorkload
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

    # # Run the synthetic apps and profile them with Allinea
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

    test_synth_app_0()
