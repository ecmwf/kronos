#!/usr/bin/env python2.7
"""
Kronos data ingestion tool.

Given a path, ingests the available data to produce a kronos cache file. This cache file may be used in other
elements of the kronos process.
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from logreader.base import LogReader

try:
    import cPickle as pickle
except:
    import pickle
import argparse

import logreader

if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("path", type=str,
                        help="The path of the data to ingest (either directory or filename). Operates recursively "
                             "on directories.")
    parser.add_argument("--type", "-t", type=str, choices=logreader.ingest_types,
                        help="The type of files to be ingested.")
    parser.add_argument("--pattern", "-p", type=str, help="Glob to match files to ingest. Defaults vary by type.")
    parser.add_argument("--output", "-o", default="parse_cache", type=str,
                        help="The output file to store the cached, ingested dataset.")
    parser.add_argument("--workers", "-w", default=10, type=int, help="The number of worker processes to use")
    parser.add_argument("--parser", help="The darshan-parser executable to use if using darshan")
    parser.add_argument("--labeller", choices=LogReader.available_label_methods,
                        help="Select how ingested jobs are labelled by the LogReaders")

    args = parser.parse_args()

    print "ARGS", args

    if not args.type:
        print "Ingestion type is required."
        print "Please supply --type=<type>"
        print "Choices: {}".format(logreader.ingest_types)
        sys.exit(-1)

    print "Ingesting file(s) associated with path: {}".format(args.path)

    ingest_config = {

        "cache": False,  # Ensure that we always actively parse if using the parsing-specific tool
        "reparse": True,
        "pool_readers": args.workers,
    }
    if args.parser:
        ingest_config['parser'] = args.parser
    if args.labeller:
        ingest_config['label_method'] = args.labeller

    print "Ingest config: {}".format(ingest_config)
    # if

    dataset = logreader.ingest_data(args.type, args.path, ingest_config=ingest_config)

    print "Ingestion complete. Writing dump file ..."
    with open(args.output, "w") as f:
        pickle.dump(dataset, f)
    print "done"
