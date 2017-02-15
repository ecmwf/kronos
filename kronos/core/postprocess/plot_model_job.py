# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import matplotlib.pyplot as plt
from kronos.core.kronos_tools.print_colour import print_colour
from kronos.core.plot_handler import PlotHandler
from kronos.core.time_signal import signal_types

priority_color_map = {
    1: (1, 0, 0),
    2: (0, 1, 0),
    3: (0, 0, 1),
    4: (1, 1, 0),
    5: (1, 1, 1),
    6: (0.5, 0.5, 0.5),
    7: (0.75, 0.25, 0.25),
    8: (0.25, 0.75, 0.75),
    9: (0.25, 0.25, 0.75),
    10: (0, 0, 0),
}


class PlotModelJob(object):
    """
    Class that plots a list of model jobs
    """

    def __init__(self, model_jobs):
        """
        This class plots a list of jobs
        :param model_jobs:
        """

        if model_jobs is not None:
            self.model_jobs = model_jobs if isinstance(model_jobs, list) else [model_jobs]
        else:
            print print_colour("orange", "provided plots is null..")

    def plot(self, save_fig_name=None, reference_metrics=None):
        """
        Plot a job. If reference metrics are passed, they are plotted too
        :param save_fig_name:
        :param reference_metrics:
        :return:
        """

        if not self.model_jobs:
            print print_colour("red", "list of plots is null, nothing to do..")
            return -1

        plot_handler = PlotHandler()

        fig_size = (20, 6)
        for cc, app in enumerate(self.model_jobs):

            plt.figure(plot_handler.get_fig_handle_ID(), figsize=fig_size, facecolor='w', edgecolor='k')
            for tt, ts_name in enumerate(signal_types.keys()):
                plt.subplot(2, len(app.timesignals.keys()), tt + 1)
                if app.timesignals[ts_name]:
                    plt.bar(app.timesignals[ts_name].xvalues,
                            app.timesignals[ts_name].yvalues,
                            0.5,
                            color=priority_color_map[app.timesignals[ts_name].priority])

                plt.xlabel(ts_name)
                plt.ylabel('')
                plt.gca().xaxis.set_major_locator(plt.NullLocator())

            # plot reference time-signals
            if reference_metrics:
                for tt, ts_name in enumerate(signal_types.keys()):
                    plt.subplot(2, len(reference_metrics.keys()), tt + 1 + len(reference_metrics.keys()))
                    plt.bar(reference_metrics[ts_name].xvalues, reference_metrics[ts_name].yvalues, 0.5, color='k')
                    plt.xlabel(ts_name)
                    plt.ylabel('')

            plt.tight_layout()

            if save_fig_name:
                plt.savefig(save_fig_name+"_{}.png".format(cc))
            plt.close()
