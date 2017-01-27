#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Quick and dirty solution to export a ksp file from dataset
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse

from kronos_io.profile_format import ProfileFormat
from logreader.dataset import IngestedDataSet


if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path_pickled", type=str, help="The path of the pickeld dataset to ingest")
    parser.add_argument("path_output", type=str, help="The path of the KPF file to write out")
    parser.add_argument("-t", "--tag", type=str, help="worklaod tag")
    args = parser.parse_args()

    dataset = IngestedDataSet.from_pickled(args.path_pickled)

    tag = args.tag if args.tag else "unknown"
    pf = ProfileFormat(model_jobs=dataset.model_jobs(), workload_tag=tag)

    with open(args.path_output, 'w') as f:
        pf.write(f)
