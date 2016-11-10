#!/usr/bin/env python

# Add parent directory as search path form modules
import os

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from config.config import Config
from kronos_model import IOWSModel
from model_workload import ModelWorkload


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
    scaling_factors = {'kb_collective': 0.9,
                       'n_collective': 0.9,
                       'n_pairwise': 0.9,
                       'kb_write': 0.9,
                       'kb_read': 0.9,
                       'flops': 0.9,
                       'kb_pairwise': 0.9,
                      }


    model = IOWSModel(config, model_jobs)
    # synapps = model.create_scaled_workload("time_plane", "Kmeans", scaling_factors)
    # synapps = model.create_scaled_workload("time_plane", "SOM", scaling_factors)
    synapps = model.create_scaled_workload("time_plane", "DBSCAN", scaling_factors)
    synthetic_apps = synapps

    # print synthetic applications stats..
    synthetic_apps.print_metrics_sums()

    # export the synthetic apps
    synthetic_apps.export(config.IOWSMODEL_TOTAL_METRICS_NBINS)


if __name__ == '__main__':

    test_modelling_cycle()
