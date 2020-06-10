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
    parser.add_argument('kill_exe', type=str, help="Path of IO worker killer application")
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

    print("Killing servers..")
    for addr in json_data:
    
        ihost = addr["host"]
        iport = addr["port"]

        _proc = subprocess.Popen([args.kill_exe, ihost, str(iport)],
                                 shell=False, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)

        print("Sent signal to kill server {} on port {}".format(ihost, iport))


