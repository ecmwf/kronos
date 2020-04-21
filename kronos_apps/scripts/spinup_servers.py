#!/usr/bin/env python3

# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import sys
import json
import argparse
import subprocess


if __name__ == "__main__":
    """
    read the host_file and spins up the servers in the 
    designated hosts.
    """

    # read other arguments if present..
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('remote_worker', type=str, help="Path of remote io worker application")
    parser.add_argument('host_file', type=str, help="Name of host file")

    # print the help if no arguments are passed
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # parse the arguments..
    args = parser.parse_args()

    # check for file existence
    if not os.path.exists(args.host_file):
        print("host file {} not found!".format(args.host_file))
        exit(1)

    with open(args.host_file, "r") as f:
        json_data = json.load(f)

    print("Spinning up servers..\n")
    for addr in json_data:
    
        ihost = addr["host"]
        iport = addr["port"]

        print("spinning up server {} on port {}".format(ihost, iport))

        # if LOG_FILE is defined, pass it on to the server exe invocation
        log_file_export = ""
        if os.getenv("LOG_FILE"):
            log_file_export  = "export LOG_FILE={}; ".format(os.getenv("LOG_FILE"))
            log_file_export += "export LOG_STREAM_FIL=1; "
            log_file_export += "export LOG_OUTPUT_LVL=1; "

        _spinup_command = "{} {} {}".format(log_file_export, args.remote_worker, iport)

        _proc = subprocess.Popen(["ssh", ihost, _spinup_command],
                                 shell=False, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)

        # print "STDOUT: {}".format(_proc.stdout.readlines())
        # print "STDERR: {}".format(_proc.stdout.readlines())


