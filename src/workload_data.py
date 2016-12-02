# from kpf_handler import KPFFileHandler
from difflib import SequenceMatcher

from kronos_tools.print_colour import print_colour
from time_signal import signal_types, TimeSignal
import numpy as np


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

    def apply_default_metrics(self, defaults_dict):
        """
        Apply passed defaults values of the time series..
        :param defaults_dict:
        :return:
        """
        print_colour('green', 'Applying default values on workload: {}'.format(self.tag))

        np.random.seed(0)
        for job in self.jobs:
            for ts_name in signal_types.keys():

                # append defaults only if the time-series is missing..
                if not job.timesignals[ts_name]:

                    # generate a random number between provided min and max values
                    y_min = defaults_dict[ts_name][0]
                    y_max = defaults_dict[ts_name][1]
                    random_y_value = y_min + np.random.rand() * (y_max - y_min)
                    job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                      xvals=0.,
                                                                      yvals=float(random_y_value),
                                                                      )

    def apply_lookup_table(self, look_up_wl, treshold):
        """
        Uses another workload as lookup table to fill missing job information
        :param workload:
        :return:
        """
        print_colour('green', 'Applying look up from workload: {} onto workload: {}'.format(look_up_wl.tag, self.tag))

        assert isinstance(look_up_wl, WorkloadData)
        assert isinstance(treshold, float)

        n_jobs_replaced = 0
        for job in self.jobs:

            # for this job looks into the provided look-up worklaod and search for best match
            # if this match exceed the treshold, then assign the timeseries to this job
            job_matches = []
            for lu_job in look_up_wl.jobs:
                job_matches.append((lu_job.job_name, SequenceMatcher(lambda x: x in "-_", job.job_name, lu_job.job_name).ratio()) )

            lu_names, lu_matches = zip(*job_matches)

            best_match_ratio = max(lu_matches)
            best_match_ratio_idx = lu_matches.index(best_match_ratio)
            best_match_name = lu_names[best_match_ratio_idx]

            # pick up the timesignals of the job for which we got the best match
            if best_match_ratio > treshold:
                print "Applying lookup metrics: from {} -----> to {}".format(best_match_name, job.job_name)
                n_jobs_replaced += 1

                # check and apply only the non None timesignals..
                for tsk in job.timesignals.keys():
                    new_ts = next(job for job in look_up_wl.jobs if job.job_name == best_match_name).timesignals[tsk]
                    if new_ts:
                        job.timesignals[tsk] = new_ts

        return n_jobs_replaced

