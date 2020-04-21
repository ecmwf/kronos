#!/usr/bin/env python

# Example of "filter" file to parse the output of the SLURM scheduler upon
# job submission and extract the job ID only.
#
# NOTE: This filter is only needed when the job dependencies are delegated
# to the scheduler (it is NOT needed when Kronos events mechanism is used).

import sys
import subprocess

if __name__ == "__main__":

    if len(sys.argv) > 1:
        command_line = " ".join(sys.argv[1:])
        return_cmd = subprocess.check_output(["sbatch"]+ sys.argv[1:])
        sys.stdout.write(return_cmd.split(" ")[-1])
    else:
        print("provide submit command!")


