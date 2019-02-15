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
        print "host file {} not found!".format(args.host_file)
        exit(1)

    with open(args.host_file, "r") as f:
        json_data = json.load(f)

    print "spinning up servers.."
    for addr in json_data:
    
        ihost = addr["host"]
        iport = addr["port"]

        _proc = subprocess.Popen(["ssh", ihost, "{} {}".format(args.remote_worker, iport)],
                                 shell=False, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)

        print "spunup server {} on port {}".format(ihost, iport)


