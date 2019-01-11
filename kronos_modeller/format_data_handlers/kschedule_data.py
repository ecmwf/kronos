# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import re

from kronos.kronos_modeller.app_kernels import available_kernels
from kronos.kronos_modeller.format_data_handlers.kschedule_kernel_work import KernelWorkDistribution
from kronos.kronos_modeller.time_signal.definitions import signal_types, time_signal_names
from kronos.kronos_modeller.tools.shared_utils import calc_histogram

from kronos_executor.io_formats.schedule_format import ScheduleFormat


class KScheduleData(ScheduleFormat):

    # maps between the available kernel names and associated parameters
    kernel_name_keys_map = {ker_class.name: [s[1] for s in ker_class.signals] for ker_class in available_kernels}

    def __init__(self,
                 sa_data_json=None,
                 sa_data=None,
                 created=None,
                 uid=None,
                 n_bins=None,
                 scaling_factors=None,
                 unscaled_metrics_sums=None):

        super(KScheduleData, self).__init__(sa_data_json=sa_data_json,
                                            sa_data=sa_data,
                                            created=created,
                                            uid=uid,
                                            n_bins=n_bins,
                                            scaling_factors=scaling_factors,
                                            unscaled_metrics_sums=unscaled_metrics_sums)

    @classmethod
    def per_kernel_series(cls, jobs, metric_name):

        _series = {k: [] for k in time_signal_names}

        for synth_app in jobs:
            for frame in synth_app["frames"]:
                for ker in frame:
                    for kernel_param in cls.kernel_name_keys_map[ker["name"]]:
                        _series[kernel_param].append(signal_types[kernel_param]["type"](ker[kernel_param]))

        return _series[metric_name]

    @classmethod
    def per_job_series(cls, jobs, metric_name):

        _series = {k: [] for k in time_signal_names}

        for synth_app in jobs:

            job_series = {k: 0 for k in time_signal_names}
            for frame in synth_app["frames"]:
                for ker in frame:
                    for kernel_param in cls.kernel_name_keys_map[ker["name"]]:
                        job_series[kernel_param] += signal_types[kernel_param]["type"](ker[kernel_param])

            for k,v in _series.iteritems():
                v.append(job_series[k])

        return _series[metric_name]

    @classmethod
    def per_process_series(cls, jobs, metric_name):

        _distribution_handler = KernelWorkDistribution(jobs)
        _series = _distribution_handler.calculate_sub_kernel_distribution(metric_name, "process")

        return _series

    @classmethod
    def per_call_series(cls, jobs, metric_name):

        _distribution_handler = KernelWorkDistribution(jobs)
        _series = _distribution_handler.calculate_sub_kernel_distribution(metric_name, "call")

        return _series

    def filter_jobs(self, re_expression=None):
        """
        Filter jobs according to job_name regex match
        :param re_expression:
        :return:
        """

        # check for non-synthetic app jobs in the schedule (they won't be included for stats)
        external_jobs = [job for job in self.synapp_data if not job.get("frames")]
        if external_jobs:
            print "[INFO]: The following jobs in kschedule are (external jobs) => not counted for statistics summary"
            print "\n".join([job["metadata"]["job_name"] for job in external_jobs])

        if not re_expression:
            return [job for job in self.synapp_data if job.get("frames")]
        else:
            return [job for job in self.synapp_data if re.match(re_expression, job["metadata"]["workload_name"]) and job.get("frames")]

    def list_job_names(self, regex=None):
        jobs = self.synapp_data if not regex else self.filter_jobs(regex)
        return [job["metadata"]["workload_name"] for job in jobs]

    @classmethod
    def get_distribution(cls, series, n_bins=10):
        return calc_histogram(series, n_bins)

    @property
    def jobs(self):
        return self.synapp_data

