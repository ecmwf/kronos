# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


class PlotHandler(object):

    """ A simple class that handles plot figures ID's"""

    fig_handle_ID = 0

    @staticmethod
    def get_fig_handle_ID():
        PlotHandler.fig_handle_ID = PlotHandler.fig_handle_ID + 1
        return PlotHandler.fig_handle_ID

    @staticmethod
    def print_fig_handle_ID():
        print "PlotHandler.fig_handle_ID: ", PlotHandler.fig_handle_ID
