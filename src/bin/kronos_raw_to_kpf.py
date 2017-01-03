#!/usr/bin/env python
"""
Quick and dirty solution to export a ksp file from dataset
"""
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse

from kronos_io.profile_format import ProfileFormat
from logreader.dataset import IngestedDataSet


if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path_pickled", type=str, help="The path of the pickeld dataset to ingest")
    parser.add_argument("path_output", type=str, help="The path of the KPF file to write out")
    args = parser.parse_args()

    dataset = IngestedDataSet.from_pickled(args.path_pickled)

    pf = ProfileFormat(model_jobs=dataset.model_jobs())

    with open(args.path_output, 'w') as f:
        pf.write(f)
