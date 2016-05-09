#!/usr/bin/env python

import sys


#////////////////////////////////////////////////////////////////
def test1_stats_ARCTUR():

    #================================================================
    ConfigOptions = Config()
    plot_tag = "test1_stats_ARCTUR"

    #================================================================
    InputWorkload = RealWorkload(ConfigOptions)
    InputWorkload.read_PBS_logs(
        "/perm/ma/maab/PBS_logs_from_ARCTUR/Arctur-1.accounting.logs")
    # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_logs_from_ARCTUR/Arctur-1.accounting.logs_test")
    # InputWorkload.read_PBS_logs("/perm/ma/maab/PBS_log_example/20151123_test100")
    InputWorkload.calculate_derived_quantities()
    InputWorkload.make_plots(plot_tag)

#////////////////////////////////////////////////////////////////


#////////////////////////////////////////////////////////////////
if __name__ == '__main__' and __package__ is None:
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from config.config import Config
    from RealWorkload import RealWorkload

    test1_stats_ARCTUR()
#////////////////////////////////////////////////////////////////
