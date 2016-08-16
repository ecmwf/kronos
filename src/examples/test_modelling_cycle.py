#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tools import mytools
from logreader import ingest_data
from config.config import Config
from IOWS_model import IOWSModel
from model_workload import ModelWorkload
from plot_handler import PlotHandler


def test_modelling_cycle():

    # Load config
    config = Config()

    # ingest allinea data
    job_datasets = []
    ingested_data = ingest_data('allinea', '/var/tmp/maab/iows/input')
    job_datasets.append(ingested_data)

    # model workload
    workload = ModelWorkload(config)
    workload.model_ingested_datasets(job_datasets)
    model_jobs = workload

    # create iows model..
    model = IOWSModel(config, model_jobs)
    synapps = model.create_scaled_workload("time_plane", "Kmeans", config.unit_sc_dict)
    synthetic_apps = synapps

    # print synthetic applications stats..
    print synapps.verbose_description()
    synthetic_apps.print_metrics_sums()

    # check num plots
    PlotHandler.print_fig_handle_ID()

    # export the synthetic apps
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)


if __name__ == '__main__':

    test_modelling_cycle()
