# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from kronos.core.time_signal.definitions import signal_types
from kronos.io.format_data_handlers.kschedule_data import KScheduleData
from kronos.shared_tools.shared_utils import mean_of_list, std_of_list


class KScheduleDataSummary(object):
    """
    Reports information from a KSchedule
    """

    _format_len = 20
    aggregation_unit = None

    def __init__(self, ksf_data, metric_name, job_match_re=None, n_bins=10):

        self.ksf_data = ksf_data
        self.metric_name = metric_name
        self.job_match_re = job_match_re
        self.n_bins = n_bins
        self.filtered_jobs = ksf_data.filter_jobs(self.job_match_re)

    def get_summary(self):

        print "\n==== SUMMARY OF {} [{}]: ({}) ====\n".format(self.metric_name.upper(),
                                                              signal_types[self.metric_name]["print_info"]["raw_units"],
                                                              signal_types[self.metric_name]["print_info"]["description"].upper())
        print "- {} jobs match the filter: {} -\n".format(len(self.filtered_jobs), self.job_match_re)

        print self.distribution_summary()

    def distribution_summary(self):

        _series = self.get_series()

        if not len(_series) or not sum(_series):
            _summary = "NO {} FOUND WITH METRIC {}".format(self.aggregation_unit.upper(), self.metric_name)
            return _summary

        _units = signal_types[self.metric_name]["print_info"]["raw_units"]

        _summary = "STATISTICS OVER {} {}S\n".format(len(_series), self.aggregation_unit.upper())

        _summary += "\nMain Statistics\n"
        _summary += "{:{_f}}:{:{_f}} [{}]\n".format("sum", sum(_series), _units, _f=self._format_len)
        _summary += "{:{_f}}:{:{_f}} [{}]\n".format("max", max(_series), _units, _f=self._format_len)
        _summary += "{:{_f}}:{:{_f}} [{}]\n".format("min", min(_series), _units, _f=self._format_len)
        _summary += "{:{_f}}:{:{_f}} [{}]\n".format("avg", mean_of_list(_series), _units, _f=self._format_len)
        _summary += "{:{_f}}:{:{_f}} [{}]\n".format("std", std_of_list(_series), _units, _f=self._format_len)

        _summary += "\nSize Distribution [{}]\n".format(_units)
        _summary += "{}\n".format("".join(["-"] * 43))
        _summary += "[{:^{_f}},{:^{_f}}]\n".format("From", "To", _f=self._format_len)
        _summary += "{}\n".format("".join(["-"]*43))

        bins, vals = self.ksf_data.get_distribution(_series, n_bins=self.n_bins)

        for bb in range(len(bins) - 1):
            _summary += "[{:{format_len}},{:{format_len}}] -> {}\n".format(bins[bb], bins[bb + 1], vals[bb], format_len=self._format_len)

        return _summary

    def get_series(self):
        raise NotImplementedError


class KScheduleDataSummaryPerKernel(KScheduleDataSummary):
    """
    Handles the per-kernel series of metrics
    """

    aggregation_unit = "kernel"

    def get_series(self):
        return KScheduleData.per_kernel_series(self.filtered_jobs, self.metric_name)


class KScheduleDataSummaryPerJob(KScheduleDataSummary):
    """
        Handles the per-job series of metrics
    """

    aggregation_unit = "job"

    def get_series(self):
        return KScheduleData.per_job_series(self.filtered_jobs, self.metric_name)


class KScheduleDataSummaryPerProcess(KScheduleDataSummary):
    """
        Handles the per-process series of metrics
    """

    aggregation_unit = "process"

    def get_series(self):
        return KScheduleData.per_process_series(self.filtered_jobs, self.metric_name)


class KScheduleDataSummaryPerCall(KScheduleDataSummary):
    """
        Handles the per-process series of metrics
    """

    aggregation_unit = "call"

    def get_series(self):
        return KScheduleData.per_call_series(self.filtered_jobs, self.metric_name)




# map between classes and their "aggregation unit" (e.g. kernel, job, etc..)
kschedule_summary_handlers = {cls.aggregation_unit: cls for cls in KScheduleDataSummary.__subclasses__()}
