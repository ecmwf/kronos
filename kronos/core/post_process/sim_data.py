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
from collections import OrderedDict

import numpy as np
import sys

from kronos.core.post_process.result_signals import ResultRunningSignal, ResultProfiledSignal
from kronos.core.time_signal.definitions import signal_types
from kronos.core.post_process.krf_data import KRFJob, krf_stats_info
from kronos.core.post_process.krf_decorator import KRFDecorator


class SimulationData(object):
    """
    Data relative to a Kronos simulation
    """

    def __init__(self, jobs=None, sim_name=None, sim_path=None, n_procs_node=None):

        # KRF jobs
        self.jobs = jobs

        # Sim-name
        self.name = sim_name

        # Sim-path
        self.path = sim_path

        # n CPU per node used for this sim
        self.n_procs_node = n_procs_node

    @classmethod
    def check_n_successful_jobs(cls, sim_path, sim_name):

        # check that the run path contains the job sub-folders
        job_dirs = [x for x in os.listdir(sim_path) if os.path.isdir(os.path.join(sim_path, x)) and "job-" in x]

        # check if there are jobs that didn't write the "statistics.krf" file
        failing_jobs = [job_dir for job_dir in job_dirs if not os.path.isfile(os.path.join(sim_path, job_dir, "statistics.krf"))]

        if failing_jobs:
            print "ERROR: The following jobs have failed (jobs for which 'statistics.krf' is not found in job folder):"
            print "{}".format("\n".join(failing_jobs))
            print "Kronos Post-processing stops here!"
            sys.exit(-1)

    @classmethod
    def read_from_sim_paths(cls, sim_path, sim_name, n_procs_node=None, permissive=False):

        print "processing simulation: {}".format(sim_name)

        # check n of successful jobs
        if not permissive:
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
            print "n successful jobs {}/{}".format(len(jobs_data), len(job_dirs))

        return cls(jobs=jobs_data, sim_name=sim_name, sim_path=sim_path, n_procs_node=n_procs_node)

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

    def class_stats(self, job_classes):
        """
        Calculate a class_stats of a simulations
        :return:
        """

        print "Aggregating class stats for sim : {}".format(self.name)

        per_class_stats = {}
        for job in self.jobs:
            job_owner_classes = job.get_class_name(job_classes)

            # add this job to any class that the job belongs to..
            for class_name in job_owner_classes:
                per_class_stats.setdefault(class_name, []).extend(job.get_stats())

        return per_class_stats

    def class_stats_sums(self, job_classes):
        """
        Calculate sums of class_stats of a simulations
        :return:
        """

        # calculate all the stats for each job class
        per_class_stats = self.class_stats(job_classes)

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

    def create_global_running_series(self, times, job_class_regex=None):
        """
        Calculate the time series for all the metrics that are "running" series (e.g. #jobs, #CPU's, #nodes)
        :param jobs:
        :param times:
        :param job_class_regex:
        :return:
        """

        bin_width = times[1] - times[0]

        t0_epoch_wl = self.tmin_epochs

        # logs number of concurrently running jobs, processes
        running_jpn = OrderedDict((
            ("jobs", np.zeros(len(times)) ),
            ("procs", np.zeros(len(times)) ),
            ("nodes", np.zeros(len(times)) ),
        ))

        # if n_proc_node is available, then also # of running nodes is calculated
        if self.n_procs_node:
            running_jpn.update({"nodes": np.zeros(len(times))})

        found = 0
        for job in self.jobs:

            if job.is_in_class(job_class_regex):

                found += 1
                first = int(math.ceil((job.t_start - t0_epoch_wl - times[0]) / bin_width))
                last = int(math.floor((job.t_end - t0_epoch_wl - times[0]) / bin_width))

                # last index should always be >= first+1
                last = last if last > first else first + 1

                # #jobs
                running_jpn["jobs"][first:last] += 1

                # #procs
                running_jpn["procs"][first:last] += job.n_cpu

                if self.n_procs_node:

                    # #nodes
                    n_nodes = job.n_cpu/int(self.n_procs_node) if not job.n_cpu%int(self.n_procs_node) else \
                        job.n_cpu/int(self.n_procs_node)+1

                    running_jpn["nodes"][first:last] += n_nodes

        # result signals (for running quantities)
        jpn_result_signals = {k: ResultRunningSignal(k, times, values) for k, values in running_jpn.iteritems()}

        return found, jpn_result_signals

    def create_global_time_series(self, times, job_class_regex=None):
        """
        Calculate time series over a specified times vector (including # processes)
        :param times:
        :param job_class_regex:
        :return:
        """

        global_time_series = {}
        bin_width = times[1] - times[0]
        found = 0
        tmin_epochs = self.tmin_epochs

        for ts_name in signal_types:

            _values = np.zeros(len(times))
            _elapsed = np.zeros(len(times))
            _processes = np.zeros(len(times))

            for jj, job in enumerate(self.jobs):

                if job.label:
                    if job.is_in_class(job_class_regex):

                        found += 1

                        if job.time_series.get(ts_name):

                            # job_ts_timestamps includes the t_0 of each interval
                            job_ts_timestamps = [0] + job.time_series[ts_name]["times"]
                            t_start = job.t_start

                            for tt in range(1, len(job_ts_timestamps)):

                                first = int(math.floor((job_ts_timestamps[tt-1] + t_start-tmin_epochs) / bin_width))
                                last = int(math.ceil((job_ts_timestamps[tt] + t_start-tmin_epochs) / bin_width))

                                # make sure that last is always > first
                                last = last if last > first else first + 1

                                # make sure that n_span_bin is >= 1
                                n_span_bin = max(1, last-first)

                                # counter to get the value corresponding to the time interval
                                value_count = tt-1

                                _values[first:last] += job.time_series[ts_name]["values"][value_count]/float(n_span_bin)
                                _elapsed[first:last] += job.time_series[ts_name]["elapsed"][value_count]/float(n_span_bin)
                                _processes[first:last] += 1

            global_time_series[ts_name] = ResultProfiledSignal(ts_name, times, _values, _elapsed, _processes)

        return found, global_time_series

    def print_job_classes_info(self, class_list, show_jobs_flag=False):
        """
        Provides basic information on job classes
        :return:
        """

        job_classes_dict = {}
        for job in self.jobs:

            classes_job_belongs_to = job.get_class_name(class_list)

            # print job-class info only if
            if show_jobs_flag:
                print "job name: {}".format(job.label)
                print "-----> belongs to classes: {}".format(classes_job_belongs_to)

            for job_class_name in classes_job_belongs_to:
                job_classes_dict.setdefault(job_class_name, []).append(job)

        print "============ SIM: {} ===============".format(self.name)

        total_jobs_in_classes = 0
        for k,v in job_classes_dict.iteritems():
            print "CLASS: {}, contains {} jobs".format(k, len(v))
            total_jobs_in_classes += len(v)

        print "total n jobs {}".format(len(self.jobs))
        print "total n in classes {}".format(total_jobs_in_classes)



