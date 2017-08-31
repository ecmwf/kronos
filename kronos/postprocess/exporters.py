# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os
import csv

from kronos.core.exceptions_iows import ConfigurationError
from kronos.postprocess.definitions import class_names_complete
import matplotlib.pyplot as plt

from kronos.postprocess.krf_data import sorted_krf_stats_names, krf_stats_info


class ExporterBase(object):

    class_export_type = "BaseExporter"
    default_export_format = None
    optional_configs = []

    def __init__(self, sim_set=None):

        # simulations data
        self.sim_set = sim_set

    def export(self, export_config, **kwargs):

        self.check_export_config(export_config)

        # Get output format and output path from config
        export_format = export_config.get("format", self.default_export_format)
        output_path = export_config.get("output_path")

        # call the export function as appropriate
        if export_format:
            self.export_function_map(export_format)(output_path, **kwargs)
        else:
            self.export_function_map(self.default_export_format)(output_path, **kwargs)

    def check_export_config(self, export_config, **kwargs):

        # create output dir if it does not exists..
        if not os.path.isdir(export_config["output_path"]):
            os.mkdir(export_config["output_path"])

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

    class_export_type = "table"
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

            csvfile = open(os.path.join(output_path, 'rates_class_{}.csv'.format(class_name.replace("/", "_"))), 'wb')
            csvwriter = csv.writer(csvfile, delimiter=',')
            csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

            # loop over the sorted metrics
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name].get(stat_name)
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

    class_export_type = "plot"
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
        exp_class_ratio_data = {}

        # ------------ find max and min rates for scaling all the plots accordingly ------------
        max_norm_value = None
        min_norm_value = None
        for class_name in class_names_complete:
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name].get(stat_name)
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
        for class_name in exp_class_ratio_data:
            fig = plt.figure(plot_id)
            plt.title('Rates class: {}'.format(class_name.replace("/", "_")))
            ax = fig.add_subplot(111)
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name].get(stat_name)
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
                plt.savefig(os.path.join(output_path, 'rates_class_{}.png'.format(class_name.replace("/", "_"))))
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


# # ///////////////////// plotting class ///////////////////
# class ExporterTimeSeries(ExporterBase):
#
#     class_export_type = "_series_tvr"
#     default_export_format = "png"
#
#     def export_function_map(self, key):
#         function_map = {
#             "png": self._export_png
#         }
#         return function_map.get(key, None)
#
#     def _export_png(self, output_path):
#         """
#         plot time series of jobs and metrics
#         :return:
#         """
#
#         print "Exporting _series_tvr plots in {}".format(output_path)
#
#         for sim_name, sim in self.sim_set.iteritems():
#
#             times_plot = np.linspace(0, sim.runtime(), 1000)
#             sim.plot_running_series(times_plot, output_path=output_path)
