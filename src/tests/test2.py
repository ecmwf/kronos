#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from config.config import Config
from IOWSModel import IOWSModel
from RealWorkload import RealWorkload
from WorkloadCorrector import WorkloadCorrector
from PlotHandler import PlotHandler

import numpy as np


def test2():

    # Load config

    config = Config()
    plot_tag = "test_NEW_"

    # Initialise the input workload
    input_workload = RealWorkload(config)
    input_workload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test_2k")
#    input_workload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    input_workload.calculate_derived_quantities()
    input_workload.make_plots(plot_tag)

    # Adjust input workload for missing fields
    # n.b. input_data is modified internal within corrector

    corrector = WorkloadCorrector(input_workload, config)
    corrector.replace_missing_data("ANN")
    # Corrector.enrich_data_with_TS("FFT")
    corrector.enrich_data_with_TS("bins")
    corrector.calculate_global_metrics()
    corrector.plot_missing_data(plot_tag)
    corrector.make_plots(plot_tag)

    # Generator model

    model = IOWSModel(config)
    model.set_input_workload(input_workload)

    # Consider clustering with differing number of clusters

    for num_clusters in np.append(np.arange(1, len(input_workload.LogData) + 1,
                                   int(len(input_workload.LogData) / 5)),
                                   len(input_workload.LogData)):
                                       
        model.apply_clustering("time_plane", "Kmeans", num_clusters)
        # model.apply_clustering("spectral", "Kmeans", num_clusters)
        # model.apply_clustering("spectral", "DBSCAN")
        model.make_plots(plot_tag)

    PlotHandler.print_fig_handle_ID()


if __name__ == '__main__':

    test2()
