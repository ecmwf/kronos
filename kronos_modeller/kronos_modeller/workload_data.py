# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import logging
import numpy as np
from kronos_exceptions import ConfigurationError
from jobs import ModelJob

from kronos_executor.definitions import signal_types, time_signal_names
from kronos_modeller.kronos_tools.utils import running_sum
from kronos_modeller.time_signal.time_signal import TimeSignal

logger = logging.getLogger(__name__)


class WorkloadData(object):
    """
    Workload data contains "jobs", each composed of standardized "model jobs"
    """

    tag_default = 0

    def __init__(self, jobs=None, tag=None):

        # a workload data contains a list of model jobs + some global properties
        self.jobs = list(jobs) if jobs else None
        self.tag = tag if tag else str(WorkloadData.tag_default+1)

        # # workload-level properties
        # self.min_start_time = None
        # self.max_start_time = None
        # self.total_metrics = None

    def __unicode__(self):
        return "WorkloadData - {} job sets".format(len(self.jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

    @staticmethod
    def from_kprofile(kprofile):
        """
        Obtain a workload from read-in and checked json data in a KProfile
        """
        return WorkloadData(
            jobs=(ModelJob.from_json(j) for j in kprofile.profiled_jobs),
            tag=kprofile.workload_tag
        )

    def append_jobs(self, model_jobs=None):
        """
        from a list of modelled jobs and a tag
        :param model_jobs:
        :return:
        """
        if not model_jobs:
            logger.info('provided an empty set oj model jobs!')
        else:
            self.jobs.append(model_jobs)

    @property
    def total_metrics_sum_dict(self):
        """
        Return a dictionary with the sum of each time signal..
        :return:
        """

        metrics_sum_dict = {}
        for ts_name in time_signal_names:
            metrics_sum_dict[ts_name] = sum(job.timesignals[ts_name].sum
                                            if job.timesignals[ts_name] else 0
                                            for job in self.jobs)

        return metrics_sum_dict

    @property
    def total_metrics_timesignals(self):
        """
        Return a dictionary with the total time signal..
        :return:
        """

        # Concatenate all the available time series data for each of the jobs
        total_metrics = {}
        for signal_name, signal_details in signal_types.iteritems():

            try:
                times_vec = np.concatenate([job.timesignals[signal_name].xvalues + job.time_start
                                            for job in self.jobs
                                            if job.timesignals[signal_name] is not None])

                data_vec = np.concatenate([job.timesignals[signal_name].yvalues
                                           for job in self.jobs
                                           if job.timesignals[signal_name] is not None])

                ts = TimeSignal.from_values(signal_name, times_vec, data_vec, base_signal_name=signal_name)
                total_metrics[signal_name] = ts

            except ValueError:
                # logger.info( "======= No jobs found with time series for {}".format(signal_name))
                pass

        return total_metrics

    @property
    def running_jobs(self):
        start_time_vec = np.asarray([sa.time_start for sa in self.jobs])
        end_time_vec = np.asarray([sa.time_start + sa.duration for sa in self.jobs])
        time_stamps, n_running_vec = running_sum(start_time_vec, end_time_vec, np.ones(end_time_vec.shape))

        return time_stamps, n_running_vec

    @property
    def running_cpus(self):
        start_time_vec = np.asarray([sa.time_start for sa in self.jobs])
        end_time_vec = np.asarray([sa.time_start + sa.duration for sa in self.jobs])
        proc_time_vec = np.asarray([sa.ncpus for sa in self.jobs])
        time_stamps, nproc_running_vec = running_sum(start_time_vec, end_time_vec, proc_time_vec)

        return time_stamps, nproc_running_vec

    @property
    def min_time_start(self):
        return min(j.time_start for j in self.jobs)

    @property
    def max_time_start(self):
        return max(j.time_start for j in self.jobs)

    def jobs_to_matrix(self, n_bins):
        """
        from jobs to ts_matrix
        :return:
        """

        ts_matrix = np.zeros((0, len(time_signal_names) * n_bins))

        # stack all the ts vectors
        for job in self.jobs:
            ts_matrix = np.vstack((ts_matrix, job.ts_to_vector(n_bins)))

        return ts_matrix

    def matrix_to_jobs(self, filled_matrix, priority, idx_map, n_bins):
        """
        from jobs to ts_matrix
        :return:
        """

        for rr, row in enumerate(filled_matrix):
            self.jobs[rr].vector_to_ts(row, priority, idx_map=idx_map, n_bins=n_bins)

    def check_jobs(self):
        """
        Check all jobs of this workload (to be used just before classification..)
        :return:
        """

        for job in self.jobs:
            job.check_job()

    def split_by_keywords(self, split_config_output):

        # extract configurations for the splitting
        new_wl_name = split_config_output['create_workload']
        split_attr = split_config_output['split_by']
        kw_include = split_config_output['keywords_in']
        kw_exclude = split_config_output['keywords_out']

        sub_wl_jobs = []
        if kw_include and not kw_exclude:
            for j in self.jobs:
                if getattr(j, split_attr):
                    if all(kw in getattr(j, split_attr) for kw in kw_include):
                        sub_wl_jobs.append(j)

        elif not kw_include and kw_exclude:
            for j in self.jobs:
                if getattr(j, split_attr):
                    if not any(kw in getattr(j, split_attr) for kw in kw_exclude):
                        sub_wl_jobs.append(j)

        elif kw_include and kw_exclude:
            sub_wl_jobs = [j for j in self.jobs if all(kw in getattr(j, split_attr) for kw in kw_include) and not
                    any(kw in getattr(j, split_attr) for kw in kw_exclude)]

            for j in self.jobs:
                if getattr(j, split_attr):
                    if all(kw in getattr(j, split_attr) for kw in kw_include) and \
                            not any(kw in getattr(j, split_attr) for kw in kw_exclude):
                        sub_wl_jobs.append(j)
        else:
            raise ConfigurationError("either included or excluded keywords are needed for splitting a workload")

        return WorkloadData(jobs=sub_wl_jobs, tag=new_wl_name)


