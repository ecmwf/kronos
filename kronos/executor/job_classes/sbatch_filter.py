#!/usr/bin/env python
# import os
import sys
import subprocess

if __name__ == "__main__":

    if len(sys.argv) > 1:
        command_line = " ".join(sys.argv[1:])
        return_cmd = subprocess.check_output(["sbatch"]+ sys.argv[1:])
        sys.stdout.write(return_cmd.split(" ")[-1])
    else:
        print "provide submit command!"


