#!/home/ma/maab/.local/miniconda2/envs/kronos/bin/python
"""
Quick and dirty solution to export a ksp file from dataset
"""
# from logreader.base import LogReader
import json
import os

from kpf_handler import KPFFileHandler

try:
    import cPickle as pickle
except:
    import pickle
import argparse
import sys

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import logreader
from logreader.dataset import IngestedDataSet
from workload_data import WorkloadData


if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", "-c", type=str, help="configuration file containing ingestion configurations")
    parser.add_argument("--kpf_filename", "-o", type=str, help="output kpf filename")
    args = parser.parse_args()

    print "ARGS", args

    if not args.config:
        print "configuration file is required."
        print "Please supply --config=<config file name (json)>"
        sys.exit(-1)

    if args.kpf_filename:
        kpf_filename = args.kpf_filename
    else:
        kpf_filename = 'workload_data.kpf'

    # read json file of config
    with open(args.config, 'r') as f:
        file_data = json.load(f)

    job_datasets = []

    # ingest datasets from pickled files if any:
    if file_data['loaded_datasets']:
        for ingest_tag, ingest_type, ingest_file in file_data['loaded_datasets']:
            print "ingesting {}".format(ingest_file)
            job_datasets.append({
                                'type': ingest_type,
                                'tag': ingest_tag,
                                'data': IngestedDataSet.from_pickled(ingest_file)
                                })

    # ingest from logs if any:
    if file_data['profile_sources']:
        for ingest_tag, ingest_type, ingest_file in file_data['profile_sources']:
            print "ingesting {}".format(ingest_file)
            job_datasets.append({
                                'type': ingest_type,
                                'tag':  ingest_tag,
                                'data': logreader.ingest_data(ingest_type, ingest_file, file_data)
                                })

    # the data from the datasets are loaded into a list of model jobs
    workloads = []
    for dataset in job_datasets:
        wl = WorkloadData(
                          jobs=[job for job in dataset['data'].model_jobs()],
                          tag=dataset['tag']
                         )
        workloads.append(wl)

    # export the workload to a kpf file
    KPFFileHandler().save_kpf(workloads, os.path.join(file_data['dir_output'], kpf_filename))
