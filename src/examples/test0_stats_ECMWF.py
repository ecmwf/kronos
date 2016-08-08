#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test0_stats_ECMWF():

    plot_dict = [
        {
            'type': 'time series',
            'title': 'ECMWF nodes and jobs (parallel jobs)',
            'subplots': ['nodes', 'jobs'],
            'queue_type': [(['np'], 'normal queue', 'b'), (['op'], 'operational queue', 'k')],
            'time format': '%a',
            'out_dir': '/var/tmp/maab/iows/output',
         },
        {
            'type': 'time series',
            'title': 'ECMWF nodes and jobs (fractional jobs)',
            'subplots': ['nodes', 'jobs'],
            'queue_type': [(['nf'], 'normal queue', 'b'), (['of'], 'operational queue', 'k')],
            'time format': '%a',
            'out_dir': '/var/tmp/maab/iows/output',
         },
        {
            'type': 'histogram',
            'n_bins': 30,
            'title': 'ECMWF histograms (parallel jobs)',
            'subplots': ['ncpus', 'run-time', 'queue-time'],
            'queue_type': [(['np'], 'normal queue', 'b'), (['op'], 'operational queue', 'k')],
            'out_dir': '/var/tmp/maab/iows/output',
         },
        {
            'type': 'histogram',
            'n_bins': 30,
            'title': 'ECMWF histograms (fractional jobs)',
            'subplots': ['ncpus', 'run-time', 'queue-time'],
            'queue_type': [(['nf'], 'normal queue', 'b'), (['of'], 'operational queue', 'k')],
            'out_dir': '/var/tmp/maab/iows/output',
         },
    ]

    scheduler_tag = "accounting"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201.csv"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test.csv"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_dict)

if __name__ == '__main__':

    test0_stats_ECMWF()
