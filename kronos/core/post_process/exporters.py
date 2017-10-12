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

from kronos.core.kronos_tools.utils import lin_reg
from kronos.core.time_signal.definitions import signal_types
from kronos.core.exceptions_iows import ConfigurationError

from kronos.core.post_process.krf_data import sorted_krf_stats_names
from kronos.core.post_process.krf_data import krf_stats_info

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
        self.export_config = None

    def export(self, export_config, output_path, job_classes, **kwargs):

        self.export_config = export_config

        self.check_export_config(export_config, output_path, job_classes, **kwargs)

        # Get output format from config (to choose appropriate method from the exporter..)
        export_format = export_config.get("format", self.default_export_format)

        # call the export function as appropriate
        if export_format:
            self.export_function_map(export_format)(output_path, job_classes, **kwargs)
        else:
            self.export_function_map(self.default_export_format)(output_path, job_classes, **kwargs)

    def check_export_config(self, export_config, out_path, job_classes, **kwargs):

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

    def _export_csv(self, output_path, job_classes, **kwargs):
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
        for class_name in job_classes:
            class_name_ser_par = job_class_string(class_name)
            csv_name = os.path.join(output_path, 'rates_class_{}.csv'.format(fig_name_from_class(class_name_ser_par)))
            csvfile = open(csv_name, 'wb')
            csvwriter = csv.writer(csvfile, delimiter=',')
            csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

            # Loop over the sorted metrics
            for stat_name in sorted_krf_stats_names:

                print "self.sim_set.ordered_sims().keys()", self.sim_set.ordered_sims().keys()

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
    optional_configs = ["plot_ylims"]

    def export_function_map(self, key):

        function_map = {
            "png": self._export_png
        }

        return function_map.get(key, None)

    def _export_png(self, output_path, job_classes, **kwargs):

        print "Exporting PNG tables in {}".format(output_path)

        # use ylims if user has provided them
        plot_ylim = self.export_config.get("plot_ylims")

        # ------------ find max and min rates for scaling all the plots accordingly ------------
        max_norm_value = None
        min_norm_value = None
        for class_name in job_classes:

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
        plot_id = 1
        for class_name in job_classes:

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

    def _export_png(self, output_path, job_classes):
        """
        plot time series of jobs and metrics
        :return:
        """

        print "Exporting time-series plots in {}".format(output_path)

        for sim in self.sim_set.sims:

            times_plot = linspace(0, sim.runtime(), 6000)
            self._make_plots(sim, times_plot, "png", job_classes, output_path=output_path)

    def _make_plots(self, sim, times_plot, fig_format, job_classes, output_path=None):
        """
        Plot time series of the simulations
        :param sim:
        :param times_plot:
        :param fig_format:
        :param output_path:
        :return:
        """

        # =====================  calculate all time-series =====================
        times_plot = np.asarray(times_plot)
        global_time_series = {}
        for cl in job_classes:
            found, series = sim.create_global_time_series(times_plot, job_class=cl)
            if found:
                global_time_series[cl] = series

        _, global_time_series["all"] = sim.create_global_time_series(times_plot)
        # =======================================================================

        # ================= Plot job and processes time-series ==================
        plt.figure(figsize=(20, 16))
        for cc, cl in enumerate(job_classes):

            cl_sp = cl[1]

            found, series_jp = running_series(sim.jobs, times_plot, sim.tmin_epochs,
                                              n_procs_node=sim.n_procs_node, job_class=cl)
            if found:
                ax = plt.subplot(3, 1, 1)
                plt.plot(times_plot, series_jp[:, 0],
                         color=job_class_color(cl, job_classes),
                         linestyle=plot_linestyle_sp[cl_sp],
                         label=job_class_string(cl))
                plt.ylabel("# jobs")
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
                if self.export_config.get("plot_ylims"):
                    if self.export_config["plot_ylims"].get("jobs"):
                        plt.ylim(self.export_config["plot_ylims"]["jobs"])

                if self.export_config.get("plot_xlims"):
                    if self.export_config["plot_xlims"].get("jobs_proc_nodes"):
                        plt.xlim(self.export_config["plot_xlims"]["jobs_proc_nodes"])

                ax = plt.subplot(3, 1, 2)
                plt.plot(times_plot, series_jp[:, 1],
                         color=job_class_color(cl, job_classes),
                         linestyle=plot_linestyle_sp[cl_sp],
                         label=job_class_string(cl))
                plt.ylabel("# procs")
                # plt.yscale('log')
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
                if self.export_config.get("plot_ylims"):
                    if self.export_config["plot_ylims"].get("procs"):
                        plt.ylim(self.export_config["plot_ylims"]["procs"])

                if self.export_config.get("plot_xlims"):
                    if self.export_config["plot_xlims"].get("jobs_proc_nodes"):
                        plt.xlim(self.export_config["plot_xlims"]["jobs_proc_nodes"])

                ax = plt.subplot(3, 1, 3)
                # print "series_jp[:, 2]", series_jp[:, 2]
                # print "series_jp.shape", series_jp.shape
                plt.plot(times_plot, series_jp[:, 2],
                         color=job_class_color(cl, job_classes),
                         linestyle=plot_linestyle_sp[cl_sp],
                         label=job_class_string(cl))
                plt.ylabel("# nodes")
                # plt.yscale('log')
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
                if self.export_config.get("plot_ylims"):
                    if self.export_config["plot_ylims"].get("nodes"):
                        plt.ylim(self.export_config["plot_ylims"]["nodes"])

                if self.export_config.get("plot_xlims"):
                    if self.export_config["plot_xlims"].get("jobs_proc_nodes"):
                        plt.xlim(self.export_config["plot_xlims"]["jobs_proc_nodes"])

        ax = plt.subplot(3, 1, 1)
        plt.title("Time series - experiment: {}".format(sim.name))
        ax.legend()

        ax = plt.subplot(3, 1, 2)
        plt.xlabel("time [s]")
        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_time_series_jobs_procs') + "."+fig_format)

        plt.close()
        # =============================================================================

        # =============== Plot all the time-series aggregated metrics =================
        # N of metrics + n of running jobs
        n_plots = len(signal_types)

        # Plot of n of *Running jobs*
        plt.figure(figsize=(20, 32))
        plt.subplot(n_plots, 1, 1)
        plt.title("Time series - experiment: {}".format(sim.name))

        pp = 0
        all_time_series = global_time_series["all"]
        for ts_name in signal_types:
            pp += 1

            times_g, values_g, _, values_p = zip(*all_time_series[ts_name])
            times_g_diff = np.diff(np.asarray(list(times_g)))
            ratios_g = np.asarray(values_g[1:]) / times_g_diff

            plt.subplot(n_plots, 1, pp)
            plt.plot(times_g[1:], ratios_g, "b")
            plt.ylabel(ts_name + " [" + labels_map[ts_name] + "]")

            if self.export_config.get("plot_ylims"):
                if self.export_config["plot_ylims"].get(ts_name):
                    plt.ylim(self.export_config["plot_ylims"][ts_name])

            if self.export_config.get("plot_xlims"):
                if self.export_config["plot_xlims"].get("metrics"):
                    plt.xlim(self.export_config["plot_xlims"]["metrics"])

            if pp == n_plots:
                plt.xlabel("time [s]")

        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_time_series') + "."+fig_format)

        plt.close()
        # =============================================================================

        # =============== Plot time-series aggregated metrics =================

        # -------- IO write
        n_t, n_v, n_e, n_p = zip(*all_time_series["n_write"])
        b_t, b_v, b_e, b_p = zip(*all_time_series["kb_write"])
        t_vals = np.asarray(n_t)
        # n_vals = np.asarray(n_v)
        b_vals = np.asarray(b_v)
        e_vals = np.asarray(b_e)
        p_vals = np.asarray(b_p)
        rates = np.asarray([b / e if e else 0.0 for b, e in zip(b_vals, e_vals)])

        plt.figure(figsize=(12, 6))
        plt.subplot(3, 1, 1)
        plt.plot(t_vals - t_vals[0], b_vals, "k")
        plt.ylabel("KiB written")
        if self.export_config.get("plot_xlims"):
            if self.export_config["plot_xlims"].get("volume_rates"):
                plt.xlim(self.export_config["plot_xlims"]["volume_rates"])

        plt.subplot(3, 1, 2)
        plt.plot(t_vals - t_vals[0], rates, "b")
        plt.ylabel("KiB/sec")
        if self.export_config.get("plot_xlims"):
            if self.export_config["plot_xlims"].get("volume_rates"):
                plt.xlim(self.export_config["plot_xlims"]["volume_rates"])

        plt.subplot(3, 1, 3)
        plt.plot(t_vals - t_vals[0], p_vals, "r")
        plt.ylabel("# writing procs")
        plt.xlabel("time [s]")
        if self.export_config.get("plot_xlims"):
            if self.export_config["plot_xlims"].get("volume_rates"):
                plt.xlim(self.export_config["plot_xlims"]["volume_rates"])

        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_time_series_io_rates') + "."+fig_format)

        plt.close()
        # =============================================================================


class ExporterScatteredData(ExporterBase):

    class_export_type = "scattered_plots"
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

        print "Exporting scattered-plots plots in {}".format(output_path)
        dt_sec = 0.1 # OK

        for sim in self.sim_set.sims:
            times_plot = linspace(0, sim.runtime(), int(sim.runtime()/dt_sec))
            self._make_plots(sim, times_plot, "png", output_path=output_path)

    def _make_plots(self, sim, times_plot, fig_format, output_path=None):
        """
        Scattered-plots metric-to-metric
        :param sim:
        :param times_plot:
        :param fig_format:
        :param output_path:
        :return:
        """

        # global time-series for all the classes
        global_time_series = {}
        _, global_time_series["all"] = sim.create_global_time_series(times_plot)

        # -------- IO write
        n_t, n_v, n_e = zip(*global_time_series["all"]["n_write"])
        b_t, b_v, b_e = zip(*global_time_series["all"]["kb_write"])
        # t_vals = np.asarray(n_t)
        n_vals = np.asarray(n_v)
        b_vals = np.asarray(b_v)
        e_vals = np.asarray(b_e)
        iowrite_idxs = np.where(e_vals <> 0) # take only values for which total elapsed time > 0
        n_vals_valid = n_vals[iowrite_idxs]
        r_vals_valid = b_vals[iowrite_idxs]/e_vals[iowrite_idxs]

        # -------- MPI collective
        mpi_coll_t, mpi_coll_v, mpi_coll_e = zip(*global_time_series["all"]["kb_collective"])
        # coll_t = np.asarray(mpi_coll_t)
        coll_v = np.asarray(mpi_coll_v)
        # coll_e = np.asarray(mpi_coll_e)

        # coll_t_valid = coll_t[iowrite_idxs]
        coll_v_valid = coll_v[iowrite_idxs]
        # coll_e_valid = coll_e[iowrite_idxs]
        highcoll_idxs = np.where(coll_v_valid > 0.5*max(coll_v_valid))
        # high_coll_r = coll_v_valid[highcoll_idxs]/coll_e_valid[highcoll_idxs]
        print "coll_v_valid", coll_v_valid
        print "len(highcoll_idxs) ",len(highcoll_idxs)


        # ------------------ bin values for average.. -----------------------
        # eps = 1e-6
        # n_bins = 10
        # t = np.asarray(n_vals_valid)
        # data = np.asarray(r_vals_valid)
        # dn = (max(t) - min(t)) / (n_bins-1)
        # bins = np.linspace(min(t)-dn/2., max(t)+dn/2., n_bins+1)
        # t_bins = (bins[1:]+bins[:-1])/2.0
        # digitized = np.digitize(t, bins)
        # bin_values = np.asarray([data[digitized == i].mean() if data[digitized == i].size else 0 for i in range(1, len(bins))])
        # -------------------------------------------------------------------

        # ------- Attempt a calculation of the regression line (forced to be > 0) --------
        sorted_vals = np.hstack( (n_vals_valid.reshape((-1,1)), r_vals_valid.reshape((-1,1)) ))
        sorted_vals = sorted_vals[sorted_vals[:, 0].argsort()]
        xraw = sorted_vals[:, 0]
        yraw = sorted_vals[:, 1]
        regression_cost, grad = lin_reg(xraw, yraw,
                                        alpha=1e-4,
                                        niter=50000,
                                        theta_0=300000,
                                        theta_1=0.0)
        regress_line_x = np.linspace(xraw[0], xraw[-1], 1000)
        regress_line_y = grad[0]*np.ones(regress_line_x.shape)+grad[1]*regress_line_x
        regress_line_y[regress_line_y < 0.0] *= 0.0

        plt.figure(figsize=(12, 6))
        plt.title("write_rate(n_write) - experiment: {}".format(sim.name))
        plt.plot(n_vals_valid, r_vals_valid, "+g",  label="Raw data")
        # plt.plot(regress_line_x, regress_line_y, "b",
        #          linestyle="-",
        #          label="Linear regression")
        plt.plot(n_vals_valid[highcoll_idxs], r_vals_valid[highcoll_idxs], "m.",
                 marker="o",
                 label="High MPI coll (>50% max)")
        plt.xlabel("# n")
        plt.ylabel("# write-rate [KiB/s]")
        if self.export_config.get("plot_xlims"):
            plt.xlim(self.export_config["plot_xlims"])
        plt.legend()
        if output_path:
            plt.savefig(os.path.join(output_path, sim.name + '_scattered_plot_n_bw') + "."+fig_format)

        # # IO rates vs MPI collective traffic
        # plt.figure(figsize=(20, 10))
        # plt.title("write_rate(MPI_collective) - experiment: {}".format(sim.name))
        # plt.plot(n_vals_valid, r_vals_valid, "+g",  label="Raw data")
        # plt.xlabel("# n")
        # plt.ylabel("# write-rate [KiB/s]")
        # if self.export_config.get("plot_xlims"):
        #     plt.xlim(self.export_config["plot_xlims"])
        # plt.legend()
        # if output_path:
        #     plt.savefig(os.path.join(output_path, sim.name + '_scattered_plot_n_bw') + "."+fig_format)
