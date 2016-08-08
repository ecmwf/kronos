#!/usr/bin/env python

import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from plotter.plotter import Plotter


#////////////////////////////////////////////////////////////////
def test2_stats_EPCC():

    plot_settings = {'run_tag': 'EPCC',
                     'out_dir': '/var/tmp/maab/iows/output',
                     'plots': [
                                {
                                    'type': 'time series',
                                    'title': 'cpus and jobs (parallel)',
                                    'subplots': ['nodes', 'jobs'],
                                    'queue_type': [(['debug', 'largemem', 'long', 'low', 'parallel', 'serial', 'standard'], '', 'b')],
                                    'time format': '%b',
                                    'out_dir': '/var/tmp/maab/iows/output',
                                },
                                 {
                                     'type': 'time series',
                                     'title': 'cpus and jobs (serial)',
                                     'subplots': ['nodes', 'jobs'],
                                     'queue_type': [(['serial'],
                                                     'normal queue', 'b')],
                                     'time format': '%b',
                                     'out_dir': '/var/tmp/maab/iows/output',
                                 },
                                {
                                    'type': 'histogram',
                                    'n_bins': 30,
                                    'title': 'parallel',
                                    'subplots': ['cpus', 'nodes', 'run-time', 'queue-time'],
                                    'queue_type': [(['debug', 'largemem', 'long', 'low', 'parallel', 'standard'], '', 'b')],
                                    'out_dir': '/var/tmp/maab/iows/output',
                                },
                                 {
                                     'type': 'histogram',
                                     'n_bins': 30,
                                     'title': 'serial',
                                     'subplots': ['cpus', 'nodes', 'run-time', 'queue-time'],
                                     'queue_type': [(['serial'], '', 'b')],
                                     'out_dir': '/var/tmp/maab/iows/output',
                                 },
                            ]
                     }

    scheduler_tag = "epcc_csv"
    scheduler_log_file = "/var/tmp/maab/iows/input/2015_ARCHER.csv"

    aPlotter = Plotter(ingest_data(scheduler_tag, scheduler_log_file))
    aPlotter.make_plots(plot_settings)


if __name__ == '__main__':

    test2_stats_EPCC()

