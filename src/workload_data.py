import numpy as np
from difflib import SequenceMatcher

# from kpf_handler import KPFFileHandler
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

        # workload-level properties
        self.min_start_time = None
        self.max_start_time = None
        self.total_metrics = None

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

    def total_metrics_sum_dict(self):
        """
        Return a dictionary with the sum of each time signal..
        :return:
        """

        metrics_sum_dict = {}
        for ts_name in signal_types.keys():
            metrics_sum_dict[ts_name] = sum(job.timesignals[ts_name].sum if job.timesignals[ts_name] else 0 for job in self.jobs)

        return metrics_sum_dict

    def total_metrics_timesignals(self):
        """
        Return a dictionary with the total time signal..
        :return:
        """

        self.min_start_time = min([i_job.time_start for i_job in self.jobs])
        self.max_start_time = max([i_job.time_start for i_job in self.jobs])

        # Concatenate all the available time series data for each of the jobs
        if not self.total_metrics:
            self.total_metrics = {}
            for signal_name, signal_details in signal_types.iteritems():

                try:
                    times_vec = np.concatenate([job.timesignals[signal_name].xvalues+job.time_start
                                                for job in self.jobs if job.timesignals[signal_name] is not None])

                    data_vec = np.concatenate([job.timesignals[signal_name].yvalues
                                               for job in self.jobs if job.timesignals[signal_name] is not None])

                    ts = TimeSignal.from_values(signal_name, times_vec, data_vec, base_signal_name=signal_name)
                    self.total_metrics[signal_name] = ts

                except ValueError:
                    print_colour("orange", "No jobs found with time series for {}".format(signal_name))

            return self.total_metrics

        else:  # if it's already been calculated
            return self.total_metrics

    def apply_default_metrics(self, defaults_dict, functions_config):
        """
        Apply passed defaults values of the time series..
        :param defaults_dict:
        :param functions_config:
        :return:
        """
        print_colour('green', 'Applying default values on workload: {}'.format(self.tag))

        np.random.seed(0)
        for job in self.jobs:
            for ts_name in signal_types.keys():

                # append defaults only if the time-series is missing..
                if not job.timesignals[ts_name]:

                    # if the entry is a list, generate the random number in [min, max]
                    if isinstance(defaults_dict[ts_name], list):
                        # generate a random number between provided min and max values
                        y_min = defaults_dict[ts_name][0]
                        y_max = defaults_dict[ts_name][1]
                        random_y_value = y_min + np.random.rand() * (y_max - y_min)
                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                          xvals=0.,
                                                                          yvals=float(random_y_value),
                                                                          )
                    elif isinstance(defaults_dict[ts_name], dict):
                        # this entry is specified through a function (name and scaling)

                        # find required function configuration by name
                        ff_config = [ff for ff in functions_config if ff["name"] == defaults_dict[ts_name]['function']]
                        if len(ff_config) > 1:
                            raise ConfigurationError("Error: multiple function have been named {}!".format(defaults_dict[ts_name]))
                        else:
                            ff_config = ff_config[0]

                        x_vec_norm, y_vec_norm = fillf.function_mapping[ff_config['type']](ff_config)

                        # rescale x and y according to job duration and scaling factor
                        x_vec = x_vec_norm * job.duration
                        y_vec = y_vec_norm * defaults_dict[ts_name]['scaling']

                        job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                          xvals=x_vec,
                                                                          yvals=y_vec,
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
                            if not job.timesignals[tsk]:
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
        else:
            raise ConfigurationError("matching threshold should be in [0,1], provided {} instead".format(threshold))

        return n_jobs_replaced
