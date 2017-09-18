# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import csv
import os

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FormatStrFormatter

from kronos.core.time_signal.definitions import signal_types
from kronos.core.exceptions_iows import ConfigurationError

from kronos.core.post_process.krf_data import sorted_krf_stats_names
from kronos.core.post_process.krf_data import krf_stats_info

from kronos.core.post_process.definitions import class_names_complete
from kronos.core.post_process.definitions import plot_linestyle_sp
from kronos.core.post_process.definitions import job_class_string
from kronos.core.post_process.definitions import job_class_color
from kronos.core.post_process.definitions import fig_name_from_class
from kronos.core.post_process.definitions import linspace
from kronos.core.post_process.definitions import running_series
from kronos.core.post_process.definitions import labels_map


class ExporterBase(object):

    class_export_type = "BaseExporter"
    default_export_format = None
    optional_configs = []

    def __init__(self, sim_set=None):

        # simulations data
        self.sim_set = sim_set

    def export(self, export_config, output_path, **kwargs):

        self.check_export_config(export_config, output_path, **kwargs)

        # Get output format from config (to choose appropriate method from the exporter..)
        export_format = export_config.get("format", self.default_export_format)

        # call the export function as appropriate
        if export_format:
            self.export_function_map(export_format)(output_path, **kwargs)
        else:
            self.export_function_map(self.default_export_format)(output_path, **kwargs)

    def check_export_config(self, export_config, out_path, **kwargs):

        # create output dir if it does not exists..
        if not os.path.isdir(out_path):
            os.mkdir(out_path)

        # check that export type is consistent with the class type
        if export_config["type"] != self.class_export_type:
            raise ConfigurationError("Export type {}, does not match class: {}".format(export_config["type"],
                                                                                       self.__class__.__name__))
        # check that export format is consistent with export type
        if not self.export_function_map(export_config["format"]):
            raise ConfigurationError("Format type {}, not implemented for class: {}".format(export_config["format"],
                                                                                            self.__class__.__name__))
        if not self.optional_configs and kwargs:
            raise ConfigurationError("Class: {} does not accept optional config keys!".format(self.__class__.__name__))
        else:
            if not all(k in self.optional_configs for k in kwargs.keys()):
                for k in kwargs.keys():
                    if k not in self.optional_configs:
                        print "Class: {} incompatible with config {}".format(self.__class__.__name__, k)
                raise ConfigurationError

    @classmethod
    def export_function_map(cls, keys):
        raise NotImplementedError


class ExporterTable(ExporterBase):

    class_export_type = "rates_table"
    default_export_format = "csv"
    optional_configs = []

    def export_function_map(self, key):

        function_map = {
            "csv": self._export_csv,
        }

        return function_map.get(key, None)

    def _export_csv(self, output_path, **kwargs):
        """
        export data into csv format
        :return:
        """

        print "Exporting CSV tables in {}".format(output_path)

        # export run-time tables
        with open(os.path.join(output_path, 'run_times.csv'), 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for sim_name, sim in self.sim_set.ordered_sims().iteritems():
                csvwriter.writerow([sim_name, sim.runtime()])

        # ------- write a stat file for each job class --------
        for class_name in class_names_complete:
            class_name_ser_par = job_class_string(class_name)
            csv_name = os.path.join(output_path, 'rates_class_{}.csv'.format(fig_name_from_class(class_name_ser_par)))
            csvfile = open(csv_name, 'wb')
            csvwriter = csv.writer(csvfile, delimiter=',')
            csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

            # loop over the sorted metrics
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name_ser_par][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name_ser_par].get(stat_name)
                                    else None for sim_name in self.sim_set.ordered_sims().keys()]

                # write the metrics only if actually present for this class..
                if all(metric_rate_list):
                    csvwriter.writerow([krf_stats_info[stat_name]["label_rate"]] + ["{:.3f}".format(v) for v in metric_rate_list])

            csvfile.close()

        # ------- write a stat file for all-class statistics --------
        csvfile = open(os.path.join(output_path, 'rates_class_all.csv'), 'wb')
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

        # Print the metrics
        for stat_name in sorted_krf_stats_names:

            # take list of rates if the metric is defined for this class otherwise get "-1" flag
            metric_rate_list = [self.sim_set.class_stats_sums[sim_name]["all_classes"][stat_name]["rate"]
                                if self.sim_set.class_stats_sums[sim_name]["all_classes"].get(stat_name)
                                else None for sim_name in self.sim_set.ordered_sims().keys()]

            if all(metric_rate_list):
                csvwriter.writerow([krf_stats_info[stat_name]["label_rate"]] + ["{:.3f}".format(v) for v in metric_rate_list])
        csvfile.close()


# ///////////////////// plotting class ///////////////////
class ExporterPlot(ExporterBase):

    class_export_type = "rates_plot"
    default_export_format = "png"
    optional_configs = ["plot_ylim"]

    def export_function_map(self, key):

        function_map = {
            "png": self._export_png
        }

        return function_map.get(key, None)

    def _export_png(self, output_path, **kwargs):

        print "Exporting PNG tables in {}".format(output_path)

        # use ylims if user has provided them
        plot_ylim = kwargs.get("plot_ylim")

        plot_id = 1

        # ------------ find max and min rates for scaling all the plots accordingly ------------
        max_norm_value = None
        min_norm_value = None
        for class_name in class_names_complete:

            class_name_ser_par = job_class_string(class_name)
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name_ser_par][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name_ser_par].get(stat_name)
                                    else None for sim_name in self.sim_set.ordered_sims().keys()]

                # write the metrics only if actually present for this class..
                if all(metric_rate_list):
                    try:
                        normalized_rates = [v/float(metric_rate_list[0]) for v in metric_rate_list if metric_rate_list[0]]
                    except ZeroDivisionError:
                        print "zero value encountered for metric: {} of class: {}".format(stat_name, class_name)
                    max_norm_value = max(max_norm_value, max(normalized_rates)) if max_norm_value else max(normalized_rates)
                    min_norm_value = min(min_norm_value, min(normalized_rates)) if min_norm_value else min(normalized_rates)

        # ------------ plot the rates per job class ------------
        for class_name in class_names_complete:

            class_name_ser_par = job_class_string(class_name)

            fig = plt.figure(plot_id)
            plt.title('Rates class: {}'.format(fig_name_from_class(class_name_ser_par)))
            ax = fig.add_subplot(111)
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name_ser_par][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name_ser_par].get(stat_name)
                                    else None for sim_name in self.sim_set.ordered_sims().keys()]

                # write the metrics only if actually present for this class..
                if all(metric_rate_list):

                    # NB: values are normalized against 1st simulation (of the ordered list of sims)
                    plt.plot(range(len(metric_rate_list)), [v / float(metric_rate_list[0]) for v in metric_rate_list],
                             label=krf_stats_info[stat_name]["label_sum"])
            plt.legend()
            plt.xticks(range(len(self.sim_set.ordered_sims())))
            ax.set_xticklabels(self.sim_set.ordered_sims().keys())
            if not plot_ylim:
                plt.ylim([0.9*min_norm_value, max_norm_value*1.1])
            else:
                plt.ylim(plot_ylim)

            plt.ylabel("Normalized Rates")
            if output_path:
                png_name = os.path.join(output_path, 'rates_class_{}.png'.format( fig_name_from_class(class_name_ser_par) ))
                print "saving file {}".format(png_name)
                plt.savefig(png_name)
            plot_id += 1

        # ------------ Plot for ALL the classes ---------
        fig = plt.figure(plot_id)
        plt.title('Rates all classes')
        ax = fig.add_subplot(111)
        for stat_name in sorted_krf_stats_names:

            # take list of rates if the metric is defined for this class otherwise get "-1" flag
            metric_rate_list = [self.sim_set.class_stats_sums[sim_name]["all_classes"][stat_name]["rate"]
                                if self.sim_set.class_stats_sums[sim_name]["all_classes"].get(stat_name)
                                else None for sim_name in self.sim_set.ordered_sims().keys()]

            # write the metrics only if actually present for this class..
            if all(metric_rate_list):
                # NB: values are normalized against 1st simulation (of the ordered list of sims)
                plt.plot(range(len(metric_rate_list)), [v / float(metric_rate_list[0]) for v in metric_rate_list],
                         label=krf_stats_info[stat_name]["label_sum"])
        plt.legend()
        plt.xticks(range(len(self.sim_set.ordered_sims())))
        ax.set_xticklabels(self.sim_set.ordered_sims().keys())
        if not plot_ylim:
            plt.ylim([0.9 * min_norm_value, max_norm_value * 1.1])
        else:
            plt.ylim(plot_ylim)

        plt.ylabel("Normalized Rates")
        if output_path:
            plt.savefig(os.path.join(output_path, 'rates_class_all.png'))
        plot_id += 1


# ///////////////////// plotting class ///////////////////
class ExporterTimeSeries(ExporterBase):

    class_export_type = "time_series"
    default_export_format = "png"

    def export_function_map(self, key):
        function_map = {
            "png": self._export_png
        }
        return function_map.get(key, None)

    def _export_png(self, output_path):
        """
        plot time series of jobs and metrics
        :return:
        """

        print "Exporting time-series plots in {}".format(output_path)

        for sim in self.sim_set.sims:

            times_plot = linspace(0, sim.runtime(), 1000)
            self._make_plots(sim, times_plot, "png", output_path=output_path)

    @staticmethod
    def _make_plots(sim, times_plot, fig_format, output_path=None):
        """
        Plot time series of the simulations
        :param sim:
        :param times_plot:
        :param fig_format:
        :param output_path:
        :return:
        """

        # make sure times are a numpy
        times_plot = np.asarray(times_plot)
        global_time_series = {}
        for cl in class_names_complete:
            found, series = sim.create_global_time_series(times_plot, job_class=cl)
            if found:
                global_time_series[cl] = series

        _, global_time_series["all"] = sim.create_global_time_series(times_plot)

        # ================= Plot job and processes time-series ==================
        plt.figure(figsize=(20, 12))
        for cc, cl in enumerate(class_names_complete):

            # print "////////////// plotting class: {}, color: {}".format(cl[0], job_class_color(cl))

            cl_sp = cl[1]

            found, series_jp = running_series(sim.jobs, times_plot, sim.tmin_epochs, cl)
            if found:
                ax = plt.subplot(2, 1, 1)
                plt.plot(times_plot, series_jp[:, 0],
                         color=job_class_color(cl),
                         linestyle=plot_linestyle_sp[cl_sp],
                         label=job_class_string(cl))
                plt.ylabel("# jobs")
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))

                ax = plt.subplot(2, 1, 2)
                plt.plot(times_plot, series_jp[:, 1],
                         color=job_class_color(cl),
                         linestyle=plot_linestyle_sp[cl_sp],
                         label=job_class_string(cl))
                plt.ylabel("# procs")
                plt.yscale('log')
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))

        ax = plt.subplot(2, 1, 1)
        plt.title("Time series - experiment: {}".format(sim.name))
        # ax.legend(loc='center left', bbox_to_anchor=(1, 0.8))
        ax.legend()

        ax = plt.subplot(2, 1, 2)
        plt.xlabel("time [s]")
        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_time_series_jobs_procs') + "."+fig_format)

        plt.close()
        # =============================================================================

        # ================= Finally plot all the time-series metrics ==================
        plt.figure(figsize=(32, 32))

        # N of metrics + n of running jobs
        n_plots = len(global_time_series.keys())

        # Plot of n of *Running jobs*
        plt.subplot(n_plots, 1, 1)
        plt.title("Time series - experiment: {}".format(sim.name))

        pp = 0
        all_time_series = global_time_series["all"]
        for ts_name in signal_types:
            pp += 1

            times_g, values_g = zip(*all_time_series[ts_name])
            times_g_diff = np.diff(np.asarray(list(times_g)))
            ratios_g = np.asarray(values_g[1:]) / times_g_diff

            plt.subplot(n_plots, 1, pp)
            plt.plot(times_g[1:], ratios_g, "b")
            plt.ylabel(ts_name + " [" + labels_map[ts_name] + "]")

            if pp == n_plots - 1:
                plt.xlabel("time [s]")

        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_time_series') + "."+fig_format)

        plt.close()

