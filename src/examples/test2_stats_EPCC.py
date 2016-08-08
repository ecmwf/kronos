#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test2_stats_EPCC():

    plot_dict = [
        {
            'type': 'time series',
            'title': 'EPCC cpus and jobs',
            'subplots': ['nodes', 'jobs'],
            'queue_type': [(['debug', 'largemem', 'long', 'low', 'parallel', 'serial', 'standard'], 'normal queue', 'b')],
            'time format': '%b',
            'out_dir': '/var/tmp/maab/iows/output',
        },
        {
            'type': 'histogram',
            'n_bins': 30,
            'title': 'EPCC histograms',
            'subplots': ['ncpus', 'run-time', 'queue-time'],
            'queue_type': [(['debug', 'largemem', 'long', 'low', 'parallel', 'serial', 'standard'], 'normal queue', 'b')],
            'out_dir': '/var/tmp/maab/iows/output',
        },
    ]

    scheduler_tag = "epcc_csv"
    scheduler_log_file = "/var/tmp/maab/iows/input/2015_ARCHER.csv"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_dict)


if __name__ == '__main__':

    test2_stats_EPCC()

