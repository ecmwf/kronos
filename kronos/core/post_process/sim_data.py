# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import os

import math
import numpy as np
import sys
from kronos.core.post_process.definitions import get_class_name
from kronos.core.time_signal.definitions import signal_types
from kronos.core.post_process.krf_data import KRFJob, krf_stats_info
from kronos.core.post_process.krf_decorator import KRFDecorator


class SimulationData(object):
    """
    Data relative to a Kronos simulation
    """

    def __init__(self, jobs=None, sim_name=None, sim_path=None):

        # KRF jobs
        self.jobs = jobs

        # Sim-name
        self.name = sim_name

        # Sim-path
        self.path = sim_path

    @classmethod
    def check_n_successful_jobs(cls, sim_path, sim_name):

        # check that the run path contains the job sub-folders
        job_dirs = [x for x in os.listdir(sim_path) if os.path.isdir(os.path.join(sim_path, x)) and "job-" in x]

        for job_dir in job_dirs:

            # Create output dir if it does not exists..
            if not os.path.isfile(os.path.join(sim_path, job_dir, "statistics.krf")):
                print "Simulation {}: 'statistics.krf' file not found in job folder {}".format(sim_name, job_dir)
                print "Kronos Postprocessing stops here!"
                sys.exit(-1)

    @classmethod
    def read_from_sim_paths(cls, sim_path, sim_name):

        print "processing simulation: {}".format(sim_name)

        # check n of successful jobs
        cls.check_n_successful_jobs(sim_path, sim_name)

        # check that the run path contains the job sub-folders
        job_dirs = [x for x in os.listdir(sim_path) if os.path.isdir(os.path.join(sim_path, x)) and "job-" in x]

        # and check that the collection was successful..
        if not job_dirs:
            print "Specified path does not contain any job folder (<job-ID>..)!"
            sys.exit(-1)

        # jobs_data_dict = {}
        jobs_data = []
        for job_dir in job_dirs:

            sub_dir_path_abs = os.path.join(sim_path, job_dir)
            sub_dir_files = os.listdir(sub_dir_path_abs)
            krf_file = [f for f in sub_dir_files if f.endswith('.krf')]

            if krf_file:
                input_file_path_abs = os.path.join(sub_dir_path_abs, 'input.json')
                stats_file_path_abs = os.path.join(sub_dir_path_abs, 'statistics.krf')

                # Read decorator data from input file
                with open(input_file_path_abs, 'r') as f:
                    json_data_input = json.load(f)
                decorator = KRFDecorator(**json_data_input["metadata"])

                # Append the profiled job to the "jobs_data" structure
                jobs_data.append(
                    KRFJob.from_krf_file(stats_file_path_abs, decorator=decorator)
                )

        if jobs_data:
            print "n successful jobs {}".format(len(jobs_data))

        return cls(jobs=jobs_data, sim_name=sim_name, sim_path=sim_path)

    def runtime(self):
        """
        Return the dt of the whole simulation
        :return:
        """
        return self.tmax_epochs - self.tmin_epochs

    @property
    def tmax_epochs(self):
        return max([j.t_start + j.duration for j in self.jobs])

    @property
    def tmin_epochs(self):
        return min([j.t_start for j in self.jobs])

    def class_stats(self):
        """
        Calculate a class_stats of a simulations
        :return:
        """

        print "Aggregating class stats for sim : {}".format(self.name)

        per_class_stats = {}
        for job in self.jobs:
            class_name = get_class_name(job.label)
            per_class_stats.setdefault(class_name, []).extend(job.get_stats())

        return per_class_stats

    def class_stats_sums(self):
        """
        Calculate sums of class_stats of a simulations
        :return:
        """

        print "Summing class stats for sim : {}".format(self.name)

        # calculate all the stats for each job class
        per_class_stats = self.class_stats()

        # init class stats sum structure
        per_class_stats_sums = {}

        # add a key that contains metric rates from all the classes
        _all_class_stats_dict = {stat_metric: {field: 0.0 for field in krf_stats_info[stat_metric]["to_sum"]}
                                 for stat_metric in krf_stats_info.keys()}

        # update stats sums
        for class_name, stats_list in per_class_stats.iteritems():

            # initialize all the requested sums..
            _class_stats_dict = {stat_metric: {field: 0.0 for field in krf_stats_info[stat_metric]["to_sum"]}
                                 for stat_metric in krf_stats_info.keys()}

            # loop over stats and make the sums
            for stat_entry in stats_list:
                for stat_metric in stat_entry.keys():
                    for field in krf_stats_info[stat_metric]["to_sum"]:

                        # add the summable metrics to class stats
                        _class_stats_dict[stat_metric][field] += float(stat_entry[stat_metric][field])

                        # add the summable metrics to all-class stats
                        _all_class_stats_dict[stat_metric][field] += float(stat_entry[stat_metric][field])

            # Filter the fields for which aggregated time is still 0 (no operations have been summed up)
            for stat_metric in _class_stats_dict.keys():
                if _class_stats_dict[stat_metric]["elapsed"] == 0.0:
                    _class_stats_dict.pop(stat_metric)

            # also calculate the rates (according to the fields defined in krf_stats_info)
            for stat_metric in _class_stats_dict.keys():

                # numerator and denominator for rate calculation
                num, den = krf_stats_info[stat_metric]["def_rate"]

                # conversion factor
                fc = krf_stats_info[stat_metric]["conv"]

                # get the rate
                rate = fc * _class_stats_dict[stat_metric][num] / _class_stats_dict[stat_metric][den]

                _class_stats_dict[stat_metric]["rate"] = rate

            # collect and store per-class stats
            per_class_stats_sums[class_name] = _class_stats_dict

        # ------- clean-up the all-class data -------
        # Filter the fields for which aggregated time is still 0 (no operations have been summed up)
        for stat_metric in _all_class_stats_dict.keys():
            if _all_class_stats_dict[stat_metric]["elapsed"] == 0.0:
                _all_class_stats_dict.pop(stat_metric)

        # also calculate the rates (according to the fields defined in krf_stats_info)
        for stat_metric in _all_class_stats_dict.keys():

            # numerator and denominator for rate calculation
            num, den = krf_stats_info[stat_metric]["def_rate"]

            # conversion factor
            fc = krf_stats_info[stat_metric]["conv"]

            # get the rate
            rate = fc * _all_class_stats_dict[stat_metric][num] / _all_class_stats_dict[stat_metric][den]

            _all_class_stats_dict[stat_metric]["rate"] = rate

        # collect and store the all-class stats
        per_class_stats_sums["all_classes"] = _all_class_stats_dict

        return per_class_stats_sums

    def create_global_time_series(self, times, class_name_root=None, serial_or_par=None):
        """
        Calculate time series over a specified times vector
        :param times:
        :param class_name_root:
        :param serial_or_par:
        :return:
        """
    
        global_time_series = {}
        bin_width = times[1] - times[0]
        found = 0
        tmin_epochs = self.tmin_epochs

        class_name_all = class_name_root+" "+serial_or_par if serial_or_par else "all"

        for ts_name in signal_types:

            running_tsvalue = np.zeros(len(times))

            for jj, job in enumerate(self.jobs):

                if job.label:

                    if ((class_name_root
                         and serial_or_par
                         and class_name_root in job.label
                         and serial_or_par in job.label) or (not class_name_root and not serial_or_par)):

                        found += 1

                        if job.time_series.get(ts_name):
                            job_ts_timestamps = [0] + job.time_series[ts_name]["times"]

                            for tt in range(len(job_ts_timestamps[1:])):
                                first = int(math.ceil(
                                    (job_ts_timestamps[tt - 1] + job.t_start - tmin_epochs) / bin_width))
                                last = int(math.floor((job_ts_timestamps[tt] + job.t_start - tmin_epochs) / bin_width))
                                last = first + 1 if last <= first else last
                                running_tsvalue[first:last] += job.time_series[ts_name]["values"][tt]

            global_time_series[ts_name] = zip(times, running_tsvalue)

        return found, global_time_series

    # # ///////////////////////////////// PLOT RUNNING SERIES /////////////////////////////////////////
    # def plot_running_series(self, times_plot, output_path=None):
    #
    #     # make sure times are a numpy
    #     times_plot = np.asarray(times_plot)
    #
    #     global_time_series = {}
    #     for cl in list_classes:
    #         for ser_par in ["serial", "parallel"]:
    #
    #             found, series = self.create_global_time_series(times_plot,
    #                                                            class_name_root=cl,
    #                                                            serial_or_par=ser_par)
    #
    #             if found:
    #                 global_time_series[cl + "/" + ser_par] = series
    #
    #     found, global_time_series["all"] = self.create_global_time_series(times_plot)
    #
    #     # Finally plot all the time-series
    #     plt.figure(figsize=(32, 32))
    #     plt.title("Time series - experiment: {}".format(self.name))
    #
    #     # N of metrics + n of running jobs
    #     n_plots = len(global_time_series.keys()) + 1
    #
    #     # Plot of n of *Running jobs*
    #     ax = plt.subplot(n_plots, 1, 1)
    #
    #     for cc, cl in enumerate(list_classes):
    #
    #         found, series = running_series(self.jobs, times_plot, self.tmin_epochs, cl, "parallel")
    #         if found:
    #             plt.plot(times_plot, series, color=class_colors[cc], linestyle="-", label=cl + "/parallel")
    #
    #         found, series = running_series(self.jobs, times_plot, self.tmin_epochs, cl, "serial")
    #         if found:
    #             plt.plot(times_plot, series, color=class_colors[cc], linestyle="--", label=cl + "/serial")
    #
    #         plt.ylabel("# running jobs")
    #     ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #
    #     pp = 1
    #     all_time_series = global_time_series["all"]
    #     for ts_name in signal_types:
    #         pp += 1
    #
    #         times_g, values_g = zip(*all_time_series[ts_name])
    #         times_g_diff = np.diff(np.asarray(list(times_g)))
    #         ratios_g = np.asarray(values_g[1:]) / times_g_diff
    #
    #         plt.subplot(n_plots, 1, pp)
    #         plt.plot(times_g[1:], ratios_g, "b")
    #         plt.ylabel(ts_name + " [" + labels_map[ts_name] + "]")
    #
    #         if pp == n_plots - 1:
    #             plt.xlabel("time [s]")
    #
    #     if output_path:
    #         plt.savefig(os.path.join(output_path, self.name + '_time_series.png'))
    #
    #     plt.close()
