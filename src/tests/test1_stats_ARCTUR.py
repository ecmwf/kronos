#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from config.config import Config
from IOWS_model import IOWSModel
from real_workload import RealWorkload
from workload_corrector import WorkloadCorrector
from plot_handler import PlotHandler


#////////////////////////////////////////////////////////////////
def test1_stats_ARCTUR():

    # #================================================================
    # ConfigOptions = Config()
    # plot_tag = "test1_stats_ARCTUR"
    #
    # #================================================================
    # InputWorkload = RealWorkload(ConfigOptions)
    # InputWorkload.read_pbs_logs("/perm/ma/maab/PBS_logs_from_ARCTUR/Arctur-1.accounting.logs")
    # # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_logs_from_ARCTUR/Arctur-1.accounting.logs_test")
    # # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    # InputWorkload.calculate_derived_quantities()
    # InputWorkload.make_plots(plot_tag)


    # Load config
    config = Config()
    plot_tag = "ARCTUR"

    scheduler_tag = "pbs"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ARCTUR/Arctur-1.accounting.logs"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ARCTUR/Arctur-1.accounting.logs_test"
    profiler_tag = ""
    profiler_log_dir = ""

    # Initialise the input workload
    input_workload = RealWorkload(config)
    input_workload.plot_tag = plot_tag
    input_workload.plot_time_tick = "year"
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)


if __name__ == '__main__' and __package__ is None:

    test1_stats_ARCTUR()

