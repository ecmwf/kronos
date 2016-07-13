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
"""

import json
import sys


class IOWS(object):
    """
    The primary IOWS application
    """

    def __init__(self, config):
        """
        TODO: Parse the config for validity --> If there are issues, that should be picked up here.
        """
        self.config = config.copy()

    def run(self):
        print "Here we are!!!"


if __name__ == "__main__":

    input_file = "input.json" if len(sys.argv) == 1 else sys.argv[1]
    print "Using configuration file: {}".format(input_file)

    try:
        with open(input_file, 'r') as f:
            config = json.load(f)

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


