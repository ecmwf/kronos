# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import numpy as np

from kronos.core.post_process.exportable_types import ExportableSignalFrame, ExportableSignalGroup
from kronos.core.post_process.exporter_base import ExporterBase
from kronos.core.post_process.definitions import job_class_color
from kronos.core.post_process.definitions import linspace
from kronos.core.post_process.definitions import labels_map
from kronos.core.post_process.result_signals import ResultInstantRatesSignal


class ExporterTimeSeries(ExporterBase):
    """
    This class plots the time-series of the various CPU/MPI/IO metrics
    """

    class_export_type = "time_series"

    def do_export(self, export_config, output_path, job_classes, **kwargs):
        """
        plot time series of metrics
        :return:
        """
        print "Exporting time-series plots in {}".format(output_path)
        for sim in self.sim_set.sims:

            # time bins (default value is one per second)
            times_plot = linspace(0, sim.runtime(), export_config.get("nbins", int(sim.runtime())))
            self.do_export_single_sim(sim, times_plot, job_classes, export_config["format"], output_path)

    def do_export_single_sim(self, sim, times_plot, job_classes, export_format, output_path):
        """
        Plot time series of the simulations
        :param sim:
        :param times_plot:
        :param job_classes:
        :param export_format:
        :param output_path:
        :return:
        """

        # ------------------  calculate all time-series ------------------------
        times_plot = np.asarray(times_plot)
        signals = {}
        for cl_name, cl_regex in job_classes.items()+[("all-classes", None)]:

            # time series of result metrics
            found_jobs_in_class, series = sim.create_global_time_series(times_plot, job_class_regex=cl_regex)

            # if some jobs in this class have been found:
            if found_jobs_in_class:

                # append all the signals corresponding to the various metrics
                signals[cl_name] = series

                # add also the "instant rate" type of signals (for IO and MPI..)
                signals[cl_name]["write_rates"] = ResultInstantRatesSignal("write_rates",
                                                                           signals[cl_name]["n_write"],
                                                                           signals[cl_name]["kb_write"])

                signals[cl_name]["read_rates"] = ResultInstantRatesSignal("read_rates",
                                                                          signals[cl_name]["n_read"],
                                                                          signals[cl_name]["kb_read"])

            # add the running signals
            found_jobs_in_class, running_series = sim.create_global_running_series(times_plot, job_class_regex=cl_regex)
            if found_jobs_in_class:
                signals[cl_name].update(running_series)

        # ------------------ assemble frame for export.. ------------------
        pp = 0
        config_lines = self.export_config["signals"]
        n_groups = len(config_lines)
        groups = []

        for ts_name, ts_options in config_lines.iteritems():

            pp += 1

            all_classes_signal = signals["all-classes"][ts_name]

            # assign plot descriptors
            all_classes_signal.name = "all-classes"
            all_classes_signal.color = "k"

            # append the "all-classes" signal
            group_signals = [all_classes_signal.get_exportable()]

            # Then append all the other signals to the group
            for cl_name, cl_regex in job_classes.iteritems():
                signal = signals[cl_name][ts_name]
                signal.name = cl_name
                signal.color = job_class_color(cl_name, job_classes.keys())
                group_signals.append(signal.get_exportable())

            # Aggregate all the signals in a subplot
            groups.append(ExportableSignalGroup(group_signals,
                                                name=ts_name,
                                                xlims=ts_options.get("x_lims"),
                                                ylims=ts_options.get("y_lims"),
                                                x_label="time [s]" if pp == n_groups else None,
                                                y_label=ts_name+labels_map[ts_name],
                                                legend=True if pp == n_groups else False))

        # Aggregate all the groups in the Exportable data structure
        export_frame = ExportableSignalFrame(groups,
                                             title="Time series - simulation: {}".format(sim.name),
                                             save_filename=os.path.join(output_path, sim.name + '_time_series'),
                                             stretch_plot=1.4
                                             )
        export_frame.export(export_format)
