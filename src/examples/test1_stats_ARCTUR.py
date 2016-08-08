#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test1_stats_ARCTUR():

    plot_dict = [
        {
            'type': 'time series',
            'title': 'ARCTUR cpus and jobs',
            'subplots': ['ncpus', 'jobs'],
            'queue_type': [(['fat', 'highprio', 'medium', 'small'], 'normal queue', 'b')],
            'time format': '%Y',
            'out_dir': '/var/tmp/maab/iows/output',
        },
        {
            'type': 'histogram',
            'n_bins': 30,
            'title': 'ARCTUR histograms',
            'subplots': ['ncpus', 'run-time', 'queue-time'],
            'queue_type': [(['fat', 'highprio', 'medium', 'small'], 'normal queue', 'b')],
            'out_dir': '/var/tmp/maab/iows/output',
        },
    ]

    scheduler_tag = "pbs"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ARCTUR/Arctur-1.accounting.logs"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_dict)


if __name__ == '__main__' and __package__ is None:

    test1_stats_ARCTUR()

