from pylab import *


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
