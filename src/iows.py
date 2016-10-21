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
        "profile_sources": [...],   # A list of profiling sources in the format described below
        "verbose": False,           # If True, output lots of data for debugging purposes

        "model_clustering", "none"  # Select the scope of the model profiling. Choices are "none",
                                    # "spectral" and "time_plane"

        "model_clustering_algorithm": ...,
                                    # Select the algorithm used or clustering. Currently "Kmeans", "SOM" or "DBSCAN".
                                    # See clustering/__init__.py
    }

    profile_sources:
      A list of tuples describing an arbitrary number of profiling sources to be combined. Each tuple
      contains (a) a key to describe what type of data this is, and (b) a path to find the data.

      ["allinea", path]    - Use data in the format produced by the Allinea MAP tool, in conjunction with
                             the map2json script. The path may be either a .json file, or a path to a
                             directory in which all files matching *.json will be considered.
      ["pbs", path]        - Use data from the PBS logs.
      ["accounting", path] - Use data from the HPC accounting logs
      ["darshan", path]    - Use darshan logs (n.b. multiple logs per job, sorted by directory)
      ["ipm", path]        - Use ipm logs (n.b. multiple logs per job, sorted by directory)
"""

import sys
import logreader
import plugins
import argparse

from exceptions_iows import ConfigurationError
from model_workload import ModelWorkload
from config.config import Config
from IOWS_model import IOWSModel


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
        self.model_jobs = None
        self.synthetic_apps = None

        if self.config.verbose:
            print "VERBOSE logging enabled"

    def ingest_data(self):
        """
        Depending on the supplied config, ingest data of various types.
        """
        self.job_datasets = []
        for ingest_type, ingest_path in self.config.profile_sources:
            self.job_datasets.append(logreader.ingest_data(ingest_type, ingest_path, self.config))

    def model_workload(self):
        workload = ModelWorkload(self.config)
        workload.model_ingested_datasets(self.job_datasets)
        self.model_jobs = workload

        if self.config.verbose:
            print workload.verbose_description()

    def scale_workload(self):
        model = IOWSModel(self.config, self.model_jobs)
        synapps = model.create_scaled_workload("time_plane", "Kmeans", self.config.unit_sc_dict)
        self.synthetic_apps = synapps

        if self.config.verbose:
            print synapps.verbose_description()

    def export(self):
        self.synthetic_apps.export(self.config.IOWSMODEL_TOTAL_METRICS_NBINS)

    def run(self):
        """
        Main execution routine
        """
        print "\nBegining data ingestion...\n----------------------------------"
        self.ingest_data()
        print "\nIngested data sets: [\n" + ",\n".join(["    {}".format(d) for d in self.job_datasets]) +  "\n]"

        print "\nModelling ingested workload...\n----------------------------------"
        self.model_workload()
        print "Generated workload model: {}".format(self.model_jobs)

        print "\nScaling model workload...\n----------------------------------"
        self.scale_workload()
        print "Scaled workload: {}".format(self.synthetic_apps)

        print "\nOutputting synthetic app input...\n----------------------------------"
        self.export()


if __name__ == "__main__":

    # read other arguments if present..
    parser = argparse.ArgumentParser(description='Kronos software')
    parser.add_argument('input_file', type=str)
    parser.add_argument('-m', "--model", help="generate model", action='store_true')
    parser.add_argument('-r', "--run", help="run the model on HPC", action='store_true')
    parser.add_argument('-p', "--postprocess", help="postprocess run results", action='store_true')
    args = parser.parse_args()

    try:
        try:

            config = Config(config_path=args.input_file)

        except (OSError, IOError) as e:
            print "Error opening input file: {}".format(e)
            print __doc__
            sys.exit(-1)

        except ValueError as e:
            print "Error parsing the supplied input file: {}".format(e)
            sys.exit(-1)

        # And get going!!!
        app = IOWS(config)

        # if set s input, use a specific plugin
        if config.plugin:

            model = plugins.factory(config.plugin['name'], config)

            if args.model:
                model.generate_model()
            elif args.run:
                model.run()
            elif args.postprocess:
                model.postprocess()
            else:
                print "command line parsing error.."
                sys.exit(-1)

        else:
            app.run()

    except ConfigurationError as e:
        print "Error in iows configuration: {}".format(e)
