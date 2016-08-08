#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test0_stats_ECMWF():

    plot_settings = {'run_tag': 'ECMWF',
                     'out_dir': '/var/tmp/maab/iows/output',
                     'plots': [
                                {
                                    'type': 'time series',
                                    'title': 'nodes and jobs (parallel jobs)',
                                    'subplots': ['nodes', 'jobs'],
                                    'queue_type': [(['np'], 'normal queue', 'b'), (['op'], 'operational queue', 'r')],
                                    'time format': '%a',
                                 },
                                {
                                    'type': 'time series',
                                    'title': 'nodes and jobs (fractional jobs)',
                                    'subplots': ['nodes', 'jobs'],
                                    'queue_type': [(['nf'], 'normal queue', 'b'), (['of'], 'operational queue', 'r')],
                                    'time format': '%a',
                                 },
                                {
                                    'type': 'histogram',
                                    'n_bins': 30,
                                    'title': 'parallel jobs',
                                    'subplots': ['cpus', 'nodes', 'run-time', 'queue-time'],
                                    'queue_type': [(['np'], 'normal queue', 'b'), (['op'], 'operational queue', 'r')],
                                 },
                                {
                                    'type': 'histogram',
                                    'n_bins': 30,
                                    'title': 'fractional jobs',
                                    'subplots': ['cpus', 'nodes', 'run-time', 'queue-time'],
                                    'queue_type': [(['nf'], 'normal queue', 'b'), (['of'], 'operational queue', 'r')],
                                 },
                              ],
    }

    scheduler_tag = "accounting"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201.csv"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test.csv"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_settings)

if __name__ == '__main__':

    test0_stats_ECMWF()
