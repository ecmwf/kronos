# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


class ExportableSignal(object):
    """
    Aggregates the data for a signal that needs to be exported
    """
    def __init__(self, name, xvalues, yvalues, labels=None, color=None, metadata=None):

        self.name = name
        self.xvalues = np.asarray(xvalues)
        self.yvalues = np.asarray(yvalues)
        self.labels = labels
        self.color = color

        # place-holder for any additional information to print out (table export only..)
        self.metadata = metadata

    def tabulated_dict(self):
        """
        Returns information to be tabulated
        :return:
        """

        return {"x_values": self.xvalues.tolist(), "y_values": self.yvalues.tolist()} if not self.metadata \
            else {"x_values": self.xvalues.tolist(), "y_values": self.yvalues.tolist(), "info": self.metadata}


class ExportableSignalGroup(object):
    """
    A group of exportable signals (and some decorators - mainly used for export as plot)
    """

    def __init__(self,
                 signals,
                 name="unknown",
                 xlims=None,
                 ylims=None,
                 x_label=None,
                 x_ticks=None,
                 x_tick_labels=None,
                 y_label=None,
                 legend=False):

        self.signals = signals
        self.name = name
        self.xlims = xlims
        self.ylims = ylims
        self.x_ticks = x_ticks
        self.x_tick_labels = x_tick_labels
        self.x_label = x_label
        self.y_label = y_label
        self.legend = legend


class ExportableSignalFrame(object):
    """
    A framework is intended as a set of signal groups and can be exported as a plot or as a table
    """

    plot_formats = ["png", "pdf", "ps", "eps", "svg"]
    table_formats = ["json"]

    def __init__(self, subplots, title=None, save_filename=None, stretch_plot=1):

        assert isinstance(subplots, list)

        self.subplots = subplots
        self.title = title
        self.save_filename = save_filename
        self.stretch_plot = stretch_plot

    def export(self, export_format):

        if export_format in self.plot_formats:
            self.export_plot(export_format)
        elif export_format in self.table_formats:
            self.export_table(export_format)
        else:
            raise RuntimeError("Format not recognised! "
                               "\nPlot formats available: {}"
                               "\nTable formats: {}".format(self.plot_formats, self.table_formats))

    def export_plot(self, export_format):

        # N of metrics + n of running jobs
        n_subplots = len(self.subplots)
        plt.figure(figsize=(16*self.stretch_plot, 4*n_subplots))
        plt.axes([0., 0., 1., 1./self.stretch_plot])
        plt.title(self.title)

        # TODO: make the plot additive and remove the "all-classes" line

        pp = 0
        for subplot in self.subplots:
            pp += 1

            ax = plt.subplot(n_subplots, 1, pp)
            plt.ylabel(subplot.y_label)

            # Then plot all the other classes..
            for signal in subplot.signals:

                plt.subplot(n_subplots, 1, pp)

                plt.plot(signal.xvalues,
                         signal.yvalues,
                         color=signal.color,
                         label=signal.name)

            if subplot.xlims:
                plt.xlim(subplot.xlims)

            if subplot.ylims:
                plt.ylim(subplot.ylims)

            if subplot.x_label:
                plt.xlabel(subplot.x_label)

            if subplot.y_label:
                plt.ylabel(subplot.y_label)

            if subplot.x_ticks:
                plt.xticks(subplot.x_ticks)
                ax.set_xticklabels(subplot.x_tick_labels)

        ax = plt.subplot(n_subplots, 1, 1)
        plt.title(self.title)
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        if self.save_filename:
            plt.savefig(self.save_filename+"."+export_format)

        plt.close()

    def export_table(self, export_format):
        """
        Export results in a CSV table
        :return:
        """

        assert export_format == "json"

        export_data = {"title": self.title}

        for group in self.subplots:
            export_data[group.name] = {signal.name: signal.tabulated_dict() for signal in group.signals}

        with open(self.save_filename+".json", 'w') as f:
            json.dump(export_data, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

