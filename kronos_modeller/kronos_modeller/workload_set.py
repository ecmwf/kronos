from kronos_executor.definitions import signal_types
from kronos_modeller.framework.pickable import PickableObject
from kronos_modeller.synthetic_app import SyntheticApp

from kronos_modeller.workload import Workload


class WorkloadSet(PickableObject):
    """
    Class that defines a group of workloads, it contains a list of workloads and
    implements common operations that span across multiple sets of workloads
    """

    def __init__(self, workloads):
        super(WorkloadSet, self).__init__()
        self.workloads = workloads

    @classmethod
    def from_labelled_jobs(cls, wl_dict):
        return cls([Workload(jobs=v, tag=k) for k, v in wl_dict.iteritems()])

    def max_timeseries(self, n_bins=None):
        """
        Returns a dictionary with the max values of all the timesignals for all the workloads of the group
        (it might be useful for scaled plots..)
        :return:
        """

        group_timeseries = {}

        # loop over signals and retrieves workload statistics
        for ts_name in signal_types:
            values_max = 0.0
            for wl in self.workloads:
                totals = wl.total_metrics_timesignals
                if totals.get(ts_name):
                    if n_bins:
                        values_max = max(values_max, max(totals[ts_name].digitized(n_bins)[1]))
                    else:
                        values_max = max(values_max, max(totals[ts_name].yvalues))

            group_timeseries[ts_name] = values_max

        return group_timeseries

    @property
    def sum_timeseries(self):
        """
        Returns a dictionary with the max values of all the timesignals for all the workloads of the group
        (it might be useful for scaled plots..)
        :return:
        """

        group_timeseries = {}

        # loop over signals and retrieves workload statistics
        for ts_name in signal_types:
            values_sum = 0.0
            for wl in self.workloads:
                totals = wl.total_metrics_timesignals
                if totals.get(ts_name):
                    values_sum += sum(totals[ts_name].yvalues)

            group_timeseries[ts_name] = values_sum

        return group_timeseries

    @property
    def max_running_jobs(self):
        return max(max(wl.running_jobs[1]) for wl in self.workloads)

    @property
    def sum_jobs(self):
        return sum(len(wl.jobs) for wl in self.workloads)

    @property
    def max_running_cpus(self):
        return max(max(wl.running_cpus[1]) for wl in self.workloads)

    def get_workload_by_name(self, wl_name):
        """
        Retrieve a workload by name
        :param wl_name:
        :return:
        """

        try:
            return next(wl for wl in self.workloads if wl.tag == wl_name)
        except StopIteration:
            print "workload {} not found in the group!".format(wl_name)
            return None

    @property
    def tags(self):
        tags = [wl.tag for wl in self.workloads]
        tags.sort()
        return tags

    @property
    def total_duration(self):
        return max(job.time_start+job.duration for wl in self.workloads for job in wl.jobs) - \
                min(job.time_start for wl in self.workloads for job in wl.jobs)

    @property
    def min_start_time(self):
        return min(j.time_start for wl in self.workloads for j in wl.jobs)

    @property
    def max_end_time(self):
        return max(j.time_start+j.duration for wl in self.workloads for j in wl.jobs)

    def generate_synapps_from_workloads(self):

        """
        Generates synthetic apps from each model job in
        each of the workloads of the set
        :return:
        """

        # Simply convert the model workloads into synthetic apps
        synthetic_apps = []
        synapp_counter = 0
        for ww, wl in enumerate(self.workloads):
            for cc, job in enumerate(wl.jobs):
                app = SyntheticApp(
                    job_name="job-{}".format(synapp_counter),
                    time_signals=job.timesignals,
                    ncpus=job.ncpus,
                    time_start=job.time_start,
                    label="WL{}-JOB{}-ID{}".format(ww, cc, synapp_counter)
                )

                synthetic_apps.append(app)

                synapp_counter += 1

        return synthetic_apps
