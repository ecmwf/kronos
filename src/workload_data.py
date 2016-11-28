# from kpf_handler import KPFFileHandler
from difflib import SequenceMatcher

from kronos_tools import print_colour
from time_signal import signal_types, TimeSignal


class WorkloadData(object):
    """
    Workload data contains "jobs", each composed of standardized "model jobs"
    """

    tag_default = 0

    def __init__(self, jobs=None, tag=None):

        # a workload data essentially contains a list of model jobs + tag
        self.jobs = jobs if jobs else None
        self.tag = tag if tag else str(WorkloadData.tag_default+1)

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
            print_colour.print_colour('orange', 'provided an empty set oj model jobs!')
        else:
            self.jobs.append(model_jobs)

    def apply_default_metrics(self, defaults_dict):
        """
        Apply passed defaults values of the time series..
        :param defaults_dict:
        :return:
        """
        for job in self.jobs:
            for ts_name in signal_types.keys():

                # append defaults only if the time-series is missing..
                if not job.timesignals[ts_name]:
                    job.timesignals[ts_name] = TimeSignal.from_values(name=ts_name,
                                                                      xvals=0.,
                                                                      yvals=float(defaults_dict[ts_name]),
                                                                      )

    def apply_lookup_table(self, look_up_wl, treshold):
        """
        Uses another workload as lookup table to fill missing job information
        :param workload:
        :return:
        """
        for job in self.jobs:

            # for this job looks into the provided look-up worklaod and search for best match
            # if this match exceed the treshold, then assign the timeseries to this job
            job_matches = []
            for lu_job in look_up_wl:
                job_matches.append((lu_job.name, SequenceMatcher(lambda x: x in "-_", job.name, lu_job.name).ratio()) )

            lu_names, lu_matches = zip(*job_matches)

            best_match_ratio = max(lu_matches)
            best_match_ratio_idx = lu_matches.index(best_match_ratio)
            best_match_name = lu_names[best_match_ratio_idx]

            # pick up the timesignals of the job for which we got the best match
            if best_match_ratio > treshold:
                job.timesignals = next(job for job in look_up_wl.jobs if job.name == best_match_name).timesignals
