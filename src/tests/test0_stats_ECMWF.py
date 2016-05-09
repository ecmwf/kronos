#!/usr/bin/env python

import os

# Add parent directory as search path form modules
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from config.config import Config
from RealWorkload import RealWorkload


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



if __name__ == '__main__':

    test0_stats_ECMWF()
