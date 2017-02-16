# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
from difflib import SequenceMatcher

# from kpf_handler import KPFFileHandler
from data_analysis import recommender
from exceptions_iows import ConfigurationError
from kronos_tools.print_colour import print_colour
import time_signal
import fill_in_functions as fillf
from jobs import ModelJob


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
    def from_kpf(kpf):
        """
        Obtain a workload from read-in and checked json data in a KPF
        """
        return WorkloadData(
            jobs=(ModelJob.from_json(j) for j in kpf.profiled_jobs),
            tag=kpf.workload_tag
        )

    def append_jobs(self, model_jobs=None):
        """
        from a list of modelled jobs and a tag
        :param model_jobs:
        :param set_tag:
        :return:
        """
        if not model_jobs:
            print_colour('orange', 'provided an empty set oj model jobs!')
        else:
            self.jobs.append(model_jobs)

    @property
    def total_metrics_sum_dict(self):
        """
        Return a dictionary with the sum of each time signal..
        :return:
        """

        metrics_sum_dict = {}
        for ts_name in time_signal.time_signal_names:
            metrics_sum_dict[ts_name] = sum(job.timesignals[ts_name].sum if job.timesignals[ts_name] else 0 for job in self.jobs)

        return metrics_sum_dict

    @property
    def total_metrics_timesignals(self):
        """
        Return a dictionary with the total time signal..
        :return:
        """

        # Concatenate all the available time series data for each of the jobs
        total_metrics = {}
        for signal_name, signal_details in time_signal.signal_types.iteritems():

            try:
                times_vec = np.concatenate([job.timesignals[signal_name].xvalues + job.time_start
                                            for job in self.jobs if job.timesignals[signal_name] is not None])

                data_vec = np.concatenate([job.timesignals[signal_name].yvalues
                                           for job in self.jobs if job.timesignals[signal_name] is not None])

                ts = time_signal.TimeSignal.from_values(signal_name, times_vec, data_vec, base_signal_name=signal_name)
                total_metrics[signal_name] = ts

            except ValueError:
                print_colour("orange", "No jobs found with time series for {}".format(signal_name))

        return total_metrics

    @property
    def min_time_start(self):
        return min(j.time_start for j in self.jobs)

    @property
    def max_time_start(self):
        return max(j.time_start for j in self.jobs)

    def apply_default_metrics(self, defaults_dict, functions_config):
        """
        Apply passed defaults values of the time series..
        :param defaults_dict:
        :param functions_config:
        :return:
        """
        print_colour('green', 'Applying default values on workload: {}'.format(self.tag))

        metrics_dict = defaults_dict['metrics']
        np.random.seed(0)
        for job in self.jobs:
            for ts_name in time_signal.time_signal_names:

                # go-ahead only if the time-series is missing or priority is less that user key
                substitute = False
                if not job.timesignals[ts_name]:
                    substitute = True
                elif job.timesignals[ts_name].priority <= defaults_dict['priority']:
                    substitute = True
                else:
                    pass

                if substitute:

                    # if the entry is a list, generate the random number in [min, max]
                    if isinstance(metrics_dict[ts_name], list):

                        # check on the length of the list (should be 2)
                        if len(metrics_dict[ts_name]) != 2:
                            raise ConfigurationError("For metrics {} 2 values are expected for filling operation, "
                                                     "but got {} instead!".format(ts_name, len(metrics_dict[ts_name])))

                        # generate a random number between provided min and max values
                        y_min = metrics_dict[ts_name][0]
                        y_max = metrics_dict[ts_name][1]
                        random_y_value = y_min + np.random.rand() * (y_max - y_min)
                        job.timesignals[ts_name] = time_signal.TimeSignal.from_values(name=ts_name,
                                                                                      xvals=0.,
                                                                                      yvals=float(random_y_value),
                                                                                      priority=defaults_dict['priority']
                                                                                      )
                    elif isinstance(metrics_dict[ts_name], dict):
                        # this entry is specified through a function (name and scaling)

                        if functions_config is None:
                            raise ConfigurationError('user functions required but not found!')

                        # find required function configuration by name
                        ff_config = [ff for ff in functions_config if ff["name"] == metrics_dict[ts_name]['function']]
                        if len(ff_config) > 1:
                            raise ConfigurationError("Error: multiple function have been named {}!".format(metrics_dict[ts_name]))
                        else:
                            ff_config = ff_config[0]

                        x_vec_norm, y_vec_norm = fillf.function_mapping[ff_config['type']](ff_config)

                        # rescale x and y according to job duration and scaling factor
                        x_vec = x_vec_norm * job.duration
                        y_vec = y_vec_norm * metrics_dict[ts_name]['scaling']

                        job.timesignals[ts_name] = time_signal.TimeSignal.from_values(name=ts_name,
                                                                                      xvals=x_vec,
                                                                                      yvals=y_vec,
                                                                                      priority=defaults_dict['priority']
                                                                                      )
                    else:
                        raise ConfigurationError('fill in "metrics" entry should be either a list or dictionary')

    def apply_lookup_table(self, look_up_wl, threshold, priority, match_keywords):
        """
        Uses another workload as lookup table to fill missing job information
        :param look_up_wl:
        :param threshold:
        :param priority:
        :param match_keywords:
        :return:
        """
        print_colour('green', 'Applying look up from workload: {} onto workload: {}'.format(look_up_wl.tag, self.tag))

        assert isinstance(look_up_wl, WorkloadData)
        assert isinstance(threshold, float)
        assert isinstance(priority, int)
        assert isinstance(match_keywords, list)

        n_jobs_replaced = 0

        # apply matching logic (if threshold < 1.0 - so not an exact matching is sought)
        if threshold < 1.0:
            for jj, job in enumerate(self.jobs):

                if not int(jj % (len(self.jobs) / 100.)):
                    print "Scanned {}% of source jobs".format(int(jj / float(len(self.jobs)) * 100.))

                for lu_job in look_up_wl.jobs:

                    # ---------- in case of multiple keys considers tha average matching ratio -----------
                    current_match = 0
                    for kw in match_keywords:
                        if getattr(job, kw) and getattr(lu_job, kw):
                            current_match += SequenceMatcher(lambda x: x in "-_", str(getattr(job, kw)), str(getattr(lu_job, kw)) ).ratio()
                    current_match /= float(len(match_keywords))
                    # -------------------------------------------------------------------------------------

                    if current_match >= threshold:
                        n_jobs_replaced += 1
                        for tsk in job.timesignals.keys():
                            if job.timesignals[tsk].priority <= priority and lu_job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]

        # compare directly (much faster..)
        elif threshold == 1:
            for jj, job in enumerate(self.jobs):

                if not int(jj % (len(self.jobs) / 100.)):
                    print "Scanned {}% of source jobs".format(int(jj / float(len(self.jobs)) * 100.))

                for lu_job in look_up_wl.jobs:

                    if all(getattr(job, kw) == getattr(lu_job, kw) for kw in match_keywords):
                        n_jobs_replaced += 1
                        for tsk in job.timesignals.keys():

                            if not job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]
                            elif job.timesignals[tsk].priority <= priority and lu_job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]
                            else:
                                pass
        else:
            raise ConfigurationError("matching threshold should be in [0,1], provided {} instead".format(threshold))

        return n_jobs_replaced

    def apply_recommender_system(self, rs_config):
        """
        Apply a recommender system technique to the jobs of this workload
        :param rs_config:
        :return:
        """

        n_bins = rs_config['n_bins']
        priority = rs_config['priority']

        # get the total matrix fro the jobs
        ts_matrix = self.jobs_to_matrix(n_bins)

        # uses a recommender model
        recomm_sys = recommender.Recommender(ts_matrix, n_bins)
        filled_matrix, mapped_columns = recomm_sys.apply()

        # re-apply filled matrix to jobs
        self.matrix_to_jobs(filled_matrix, priority, mapped_columns, n_bins)

    def jobs_to_matrix(self, n_bins):
        """
        from jobs to ts_matrix
        :return:
        """

        ts_matrix = np.zeros((0, len(time_signal.time_signal_names) * n_bins))

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
