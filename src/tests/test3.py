#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from real_workload import RealWorkload
from workload_corrector import WorkloadCorrector
from plot_handler import PlotHandler


def test3():

    # Load config

    config = Config()
    plot_tag = "test_NEW_"

    # Initialise the input workload
    input_workload = RealWorkload(config)
    input_workload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test_2k")
    # input_workload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    input_workload.calculate_derived_quantities()
    input_workload.enrich_data_with_TS("bins")
    input_workload.calculate_global_metrics()
    input_workload.make_plots(plot_tag)

    corrector = WorkloadCorrector(input_workload, config)
    corrector.replace_missing_data("ANN")
    corrector.plot_missing_data(plot_tag)
    corrector.make_plots(plot_tag)

    # Generator model
    scaling_factor = 10  # [%] percentage of measured workload
    model = IOWSModel(config)
    model.set_input_workload(input_workload)
    model.create_scaled_workload("time_plane", "Kmeans", scaling_factor)
    model.export_scaled_workload()
    model.make_plots(plot_tag)

    # check num plots
    PlotHandler.print_fig_handle_ID()

if __name__ == '__main__':

    test3()
