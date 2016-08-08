#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test1_stats_ARCTUR():

    plot_settings = {'run_tag': 'ARCTUR',
                     'out_dir': '/var/tmp/maab/iows/output',
                     'plots': [
                                {
                                    'type': 'time series',
                                    'title': 'cpus and jobs',
                                    'subplots': ['cpus', 'jobs'],
                                    'queue_type': [(['fat', 'highprio', 'medium', 'small'], '', 'b')],
                                    'time format': '%Y',
                                },
                                {
                                    'type': 'histogram',
                                    'n_bins': 30,
                                    'title': '',
                                    'subplots': ['cpus', 'nodes', 'run-time', 'queue-time', 'cpu-hours'],
                                    'queue_type': [(['fat', 'highprio', 'medium', 'small'], '', 'b')],
                                },
                             ]
                     }

    scheduler_tag = "pbs"
    scheduler_log_file = "/perm/ma/maab/ngio_logs/ARCTUR/Arctur-1.accounting.logs"
    # scheduler_log_file = "/perm/ma/maab/ngio_logs/ARCTUR/Arctur-1.accounting.logs_test"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_settings)


if __name__ == '__main__' and __package__ is None:

    test1_stats_ARCTUR()

