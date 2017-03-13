# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np

from kronos.core.time_signal import signal_types
from kronos.core.workload_data import WorkloadData, WorkloadDataGroup
from kronos.io.profile_format import ProfileFormat


class Summary(object):

    def __init__(self, kpf_model_name, kpf_orig_name, ksf_file_name=None):

        self._summary = ""

        print "reading original KPF data.."
        # retrieve information from KPF of original workloads
        self.wl_orig_group = WorkloadDataGroup.from_pickled(kpf_orig_name)
        self.max_running_jobs_orig = self.wl_orig_group.max_running_jobs
        self.max_running_cpus_orig = self.wl_orig_group.max_running_cpus
        self.vals_max_orig = self.wl_orig_group.max_timeseries()
        self.vals_sum_orig = self.wl_orig_group.sum_timeseries

        print "reading model KPF data.."
        kpf_workload = WorkloadData.from_kpf(ProfileFormat.from_filename(kpf_model_name))
        self.n_apps = len(kpf_workload.jobs)
        self.wl_model_group = kpf_workload.group_by_job_labels
        self.max_running_jobs_model = self.wl_model_group.max_running_jobs
        self.max_running_cpus_model = self.wl_model_group.max_running_cpus
        self.vals_max_model = self.wl_model_group.max_timeseries()
        self.vals_sum_model = self.wl_model_group.sum_timeseries

        self.ksf_file_name = ksf_file_name

    def __unicode__(self):
        return "\nSummary:\n{}".format(self.print_summary)

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def print_summary(self):

        _str = ""

        # number of jobs per workload in the group
        _str += "\n======================================== JOB STATS ========================================\n"
        orig_tot_n_jobs = sum(len(wl.jobs) for wl in self.wl_orig_group.workloads)
        model_tot_n_jobs = sum(len(wl.jobs) for wl in self.wl_model_group.workloads)
        _str += "\n------------------ [ORIG:MODEL] ------------------\n\n"
        _str += "|  wl name  |    #jobs     |       %        |\n"
        for tag in self.wl_orig_group.tags:
            wl_orig = self.wl_orig_group.get_workload_by_name(tag)
            wl_model = self.wl_model_group.get_workload_by_name(tag)
            n_jobs_orig = len(wl_orig.jobs)
            n_jobs_model = len(wl_model.jobs)
            n_jobs_orig_pc = len(wl_orig.jobs) / float(orig_tot_n_jobs) * 100
            n_jobs_model_pc = len(wl_model.jobs) / float(model_tot_n_jobs) * 100
            _str += "{:12s} [{:5d}: {:5d}] [{:5.2f}%: {:5.2f}%]\n".format(tag[:10],
                                                                      n_jobs_orig,
                                                                      n_jobs_model,
                                                                      n_jobs_orig_pc,
                                                                      n_jobs_model_pc)

        _str += "\n\n===================================== RUN-TIME STATS =====================================\n"
        _str += "\n----------------------------------------- ORIG -----------------------------------------\n"
        _str += self.print_runtime_stats(self.wl_orig_group)

        _str += "\n----------------------------------------- MODEL ----------------------------------------\n"
        _str += self.print_runtime_stats(self.wl_model_group)

        _str += "\n\n====================================== METRICS STATS =====================================\n"

        _str += "\n----------------------------------------- ORIG -----------------------------------------\n"
        _str += self.print_metrics_stats(self.wl_orig_group, "cpu")
        _str += self.print_metrics_stats(self.wl_orig_group, "mpi")
        _str += self.print_metrics_stats(self.wl_orig_group, "file")

        _str += "\n----------------------------------------- MODEL ----------------------------------------\n"
        _str += self.print_metrics_stats(self.wl_model_group, "cpu")
        _str += self.print_metrics_stats(self.wl_model_group, "mpi")
        _str += self.print_metrics_stats(self.wl_model_group, "file")
        
        return _str

    def print_workload_tags(self):
        """
        Prints the tags present in the workloads
        :return:
        """
        _str = ""

        _str += "---- job statistics ------\n"
        _str += "model-group tags: {}\n".format(self.wl_model_group.tags)
        _str += "orig-group tags: {}\n".format(self.wl_orig_group.tags)

        return _str

    @staticmethod
    def print_runtime_stats(wl_group):

        _str = ""

        _str += "T_total: {:.2f} [sec]\n".format(wl_group.total_duration)
        _str += " wl name   |  T_mean  |              job T_max        |            job T_min            |\n"
        for tag in wl_group.tags:
            wl = wl_group.get_workload_by_name(tag)
            job_runtimes = [job.duration for job in wl.jobs]
            job_tmax = wl.jobs[np.argmax(job_runtimes)].job_name
            job_tmin = wl.jobs[np.argmin(job_runtimes)].job_name
            _str += "{:10s} {:10.2f} {:10.2f} ({:20s}) {:10.2f} ({:20s})\n".format(tag[:10], np.mean(job_runtimes),
                                                                               max(job_runtimes), job_tmax,
                                                                               min(job_runtimes), job_tmin)

        return _str

    @staticmethod
    def print_metrics_stats(wl_group, metric_type):

        _str = ""

        group_sums = wl_group.sum_timeseries

        _str += "\n{} metrics:\n".format(metric_type.upper())
        keys_ = [ts_name for ts_name in signal_types.keys() if metric_type.lower() in signal_types[ts_name]['category']]
        _str += "{:10s}".format("Name") + "".join("{:>28s}".format(ts_name) for ts_name in keys_)
        _str += "\n"
        for tag in wl_group.tags:
            wl = wl_group.get_workload_by_name(tag)
            wl_sums = wl.total_metrics_sum_dict
            pc_ = {ts_name: wl_sums[ts_name] / float(group_sums[ts_name]) * 100. if group_sums[ts_name] else 0. for
                   ts_name in keys_}
            _str += "{:10s}".format(tag[:10]) + "".join(
                "{:20.3e} [{:4.1f}%]".format(wl_sums[ts_name], pc_[ts_name]) for ts_name in keys_)
            _str += "\n"

        return _str

    def print_relative_n_job(self):
        """
        Prints percentage of jobs in the workload
        :return:
        """

        _str = ""

        _str += "\n\n======================================== JOB STATS ========================================\n"
        orig_tot_n_jobs = sum(len(wl.jobs) for wl in self.wl_orig_group.workloads)
        model_tot_n_jobs = sum(len(wl.jobs) for wl in self.wl_model_group.workloads)
        _str += "\n------------------ [ORIG:MODEL] ------------------\n"
        _str += "|  wl name  |    #jobs     |       %        |\n"
        for tag in self.wl_orig_group.tags:
            wl_orig = self.wl_orig_group.get_workload_by_name(tag)
            wl_model = self.wl_model_group.get_workload_by_name(tag)
            n_jobs_orig = len(wl_orig.jobs)
            n_jobs_model = len(wl_model.jobs)
            n_jobs_orig_pc = len(wl_orig.jobs)/float(orig_tot_n_jobs)*100
            n_jobs_model_pc = len(wl_model.jobs)/float(model_tot_n_jobs)*100
            _str += "{:12s} [{:5d}: {:5d}] [{:5.2f}%: {:5.2f}%]\n".format(tag[:10],
                                                                      n_jobs_orig,
                                                                      n_jobs_model,
                                                                      n_jobs_orig_pc,
                                                                      n_jobs_model_pc)

        return _str
