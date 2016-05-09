#!/usr/bin/env python

import fileinput
import subprocess
import sys
import getopt
import os


#////////////////////////////////////////////////////////////////
def test0_stats_ECMWF():

    #================================================================
    ConfigOptions = Config()
    plot_tag = "test0_stats_ECMWF"

    #================================================================
    InputWorkload = RealWorkload(ConfigOptions)
    # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123")
    InputWorkload.calculate_derived_quantities()
    InputWorkload.make_plots(plot_tag, "hour")


#////////////////////////////////////////////////////////////////


#////////////////////////////////////////////////////////////////
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from config.config import Config
    from RealWorkload import RealWorkload

    test0_stats_ECMWF()
#////////////////////////////////////////////////////////////////
