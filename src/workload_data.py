import numpy as np
from difflib import SequenceMatcher

# from kpf_handler import KPFFileHandler
from data_analysis import recommender
from exceptions_iows import ConfigurationError
from kronos_tools.print_colour import print_colour
from time_signal import signal_types, TimeSignal
import fill_in_functions as fillf


class WorkloadData(object):
    """
    Workload data contains "jobs", each composed of standardized "model jobs"
    """

    tag_default = 0

    def __init__(self, jobs=None, tag=None):

        # a workload data contains a list of model jobs + some global properties
        self.jobs = jobs if jobs else None
        self.tag = tag if tag else str(WorkloadData.tag_default+1)

        # # workload-level properties
        # self.min_start_time = None
        # self.max_start_time = None
        # self.total_metrics = None

    def __unicode__(self):
        return "WorkloadData - {} job sets".format(len(self.jobs))

    def __str__(self):
        return unicode(self).encode('utf-8')

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
        for ts_name in signal_types.keys():
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
        for signal_name, signal_details in signal_types.iteritems():

            try:
                times_vec = np.concatenate([job.timesignals[signal_name].xvalues + job.time_start
                                            for job in self.jobs if job.timesignals[signal_name] is not None])

                data_vec = np.concatenate([job.timesignals[signal_name].yvalues
                                           for job in self.jobs if job.timesignals[signal_name] is not None])

                ts = TimeSignal.from_values(signal_name, times_vec, data_vec, base_signal_name=signal_name)
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
            for ts_name in signal_types.keys():

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
                        # generate a random number between provided min and max values
                        y_min = metrics_dict[ts_name][0]
                        y_max = metrics_dict[ts_name][1]
                        random_y_value = y_min + np.random.rand() * (y_max - y_min)
                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                          xvals=0.,
                                                                          yvals=float(random_y_value),
                                                                          priority=defaults_dict['priority']
                                                                          )
                    elif isinstance(metrics_dict[ts_name], dict):
                        # this entry is specified through a function (name and scaling)

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

                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
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

                            if not job.timesignals[tsk] and lu_job.timesignals[tsk]:
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

        # uses a recommender model
        recomm_sys = recommender.Recommender()

        # train it according to a selected number of bins for each metric
        recomm_sys.train_model(self.jobs, n_ts_bins=n_bins)

        # re-apply it on the jobs
        self.jobs = recomm_sys.apply_model_to(self.jobs, priority)

    def check_jobs(self):
        """
        Check all jobs of this workload (to be used just before classification..)
        :return:
        """

        for job in self.jobs:
            job.check_job()

    def split_by_keywords(self, split_config_output):

        # extract configurations for the splitting
        split_workloads = []
        for sub_wl_config in split_config_output:

            new_wl_name = sub_wl_config['workload_name']
            split_attr = sub_wl_config['split_by'][0]
            kw_include = sub_wl_config['split_by'][1]
            kw_exclude = sub_wl_config['split_by'][2]

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

            split_workloads.append(WorkloadData(jobs=sub_wl_jobs, tag=new_wl_name))

        return split_workloads
