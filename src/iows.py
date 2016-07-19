#!/usr/bin/env python2.7

"""
IOWS data processing management tool

This tool takes initial profiling data in various forms and:

   i) Cleans it
  ii) Processes and ingests it
 iii) Models jobs and workloads
  iv) Manipulates (scales) these workloads
   v) Outputs JSONs required to power the synthetic applications

Usage:

    iows <input_file> [options]

Options:
    <None>

Input file syntax:

    The input file is a JSON. Full documentation can be found on the ECMWF confluence page.
    The file should contain one complete object, containing the following keys (with fuller explanations
    given below if required):

    {
        "profile_sources": [...]   # A list of profiling sources in the format described below
    }

    profile_sources:
      A list of tuples describing an arbitrary number of profiling sources to be combined. Each tuple
      contains (a) a key to describe what type of data this is, and (b) a path to find the data.

      ("allinea", path)    - Use data in the format produced by the Allinea MAP tool, in conjunction with
                             the map2json script. The path may be either a .json file, or a path to a
                             directory in which all files matching *.json will be considered.
      ("pbs", path)        - Use data from the PBS logs.
      ("accounting", path) - Use data from the HPC accounting logs
"""

import json
import sys
import logreader

from exceptions_iows import *
from model_workload import ModelWorkload
from config.config import Config


class IOWS(object):
    """
    The primary IOWS application
    """

    def __init__(self, config):
        """
        TODO: Parse the config for validity --> If there are issues, that should be picked up here.
        """
        self.config = config
        self.job_datasets = None

    def ingest_data(self):
        """
        Depending on the supplied config, ingest data of various types.
        """
        self.job_datasets = []
        for ingest_type, ingest_path in self.config.profile_sources:
            self.job_datasets.append(logreader.ingest_data(ingest_type, ingest_path))

    def model_workload(self):
        workload = ModelWorkload(self.config)
        workload.model_ingested_datasets(self.job_datasets)
        self.model_jobs = workload

    def run(self):
        """
        Main execution routine
        """
        print "\nBegining data ingestion...\n----------------------------------"
        self.ingest_data()
        print "Ingested data sets: [\n" + ",\n".join(["    {}".format(d) for d in self.job_datasets]) +  "\n]"

        print "\nModelling ingested workload...\n----------------------------------"
        self.model_workload()
        print "Generated workload model: {}".format(self.model_jobs)

        print "\nScaling model workload...\n----------------------------------"
        print "\nOutputting synthetic app input...\n----------------------------------"


if __name__ == "__main__":

    input_file = "input.json" if len(sys.argv) == 1 else sys.argv[1]
    print "Using configuration file: {}".format(input_file)

    try:
        try:

            config = Config(config_path=input_file)

        except (OSError, IOError) as e:
            print "Error opening input file: {}".format(e)
            print __doc__
            sys.exit(-1)

        except ValueError as e:
            print "Error parsing the supplied input file: {}".format(e)
            sys.exit(-1)

        # And get going!!!
        app = IOWS(config)
        app.run()

    except ConfigurationError as e:
        print "Error in iows configuration: {}".format(e)


