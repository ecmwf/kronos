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


    plot_dict = [
        {
         'type': 'time series',
         'title': 'ECMWF nodes and jobs (parallel jobs)',
         'subplots': ['nodes', 'jobs'],
         'queue_type': [(['np'], 'normal queue', 'b'), (['op'], 'operational queue', 'k')],
         'time format': '%d',
         'out_dir': '/var/tmp/maab/iows/output',
         },
        {
         'type': 'time series',
         'title': 'ECMWF nodes and jobs (fractional jobs)',
         'subplots': ['nodes', 'jobs'],
         'queue_type': [(['nf'], 'normal queue', 'b'), (['of'], 'operational queue', 'k')],
         'time format': '%m',
         'out_dir': '/var/tmp/maab/iows/output',
         },
    ]


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
    input_workload.read_logs(scheduler_tag, profiler_tag, scheduler_log_file, profiler_log_dir, plot_dict=plot_dict)


if __name__ == '__main__':

    test0_stats_ECMWF()
