#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from model_workload import ModelWorkload
from workload_corrector import WorkloadCorrector
from plot_handler import PlotHandler


#////////////////////////////////////////////////////////////////
def test0_stats_ECMWF():

    # #================================================================
    # ConfigOptions = Config()
    # plot_tag = "test0_stats_ECMWF"
    # #================================================================
    # InputWorkload = RealWorkload(ConfigOptions)
    # # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    # InputWorkload.read_pbs_logs("/perm/ma/maab/PBS_log_example/20151123")
    # InputWorkload.calculate_derived_quantities()
    # InputWorkload.make_plots(plot_tag, "hour")

    # Load config
    config = Config()
    plot_tag = "ECMWF"

    # scheduler_tag = "pbs"
    # scheduler_log_file = "/perm/ma/maab/PBS_log_example/20151123"
    # # scheduler_log_file = "/perm/ma/maab/PBS_log_example/20151123_test_2k"
    # profiler_tag = ""
    # profiler_log_dir = ""

    scheduler_tag = "accounting"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201.csv"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test.csv"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test_0.csv"
    profiler_tag = ""
    profiler_log_dir = ""

    # Initialise the input workload
    input_workload = ModelWorkload(config)
    input_workload.plot_tag = plot_tag
    input_workload.plot_time_tick = "day"
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)


if __name__ == '__main__':

    test0_stats_ECMWF()
