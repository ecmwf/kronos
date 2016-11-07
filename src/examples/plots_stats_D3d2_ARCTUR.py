#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter
from postprocess.statistics import Statistics


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

    ingested_data = ingest_data(scheduler_tag, scheduler_log_file)

    aPlotter = Plotter(ingested_data)
    aPlotter.make_plots(plot_settings)

    stats = Statistics()
    stats.dataset_statistics(ingested_data)


if __name__ == '__main__' and __package__ is None:

    test1_stats_ARCTUR()

