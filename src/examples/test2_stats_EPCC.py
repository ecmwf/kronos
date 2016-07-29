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
def test2_stats_EPCC():


    # Load config
    config = Config()
    plot_tag = "EPCC"

    scheduler_tag = "epcc_csv"
    # scheduler_log_file = "/var/tmp/maab/iows/input/sample_epcc.csv"
    scheduler_log_file = "/var/tmp/maab/iows/input/2015_ARCHER.csv"
    profiler_tag = ""
    profiler_log_dir = ""

    # Initialise the input workload
    input_workload = RealWorkload(config)
    input_workload.plot_tag = plot_tag
    input_workload.plot_time_tick = "month"
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir)


if __name__ == '__main__':

    test2_stats_EPCC()

