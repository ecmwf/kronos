import numpy as np
import time_signal

from exceptions_iows import ModellingError
from tools.print_colour import print_colour
from tools.time_format import format_seconds


class ModelJob(object):
    """
    A model job is as fully specified, abstract job, which is output from the combined data ingestion
    and processing units.
    """
    # Declare the fields that have been used (so they can be found by IDEs, etc.)
    time_start = None
    ncpus = None
    nnodes = None
    duration = None
    label = None

    # Times may come from the scheduler (+ associated systems), or from other profiling sources. Those from the
    # scheduler (n.b. None works as False)
    scheduler_timing = None

    # Does what it says on the tin
    required_fields = [

        'time_start',

        'ncpus',
        'nnodes'
    ]

    def __init__(self, time_series=None, **kwargs):
        """
        Set the fields as passed in
        """
        for k, v in kwargs.iteritems():
            if not hasattr(self, k):
                raise ModellingError("Setting field {} of ModelJob. Field not available.".format(k))
            if getattr(self, k) is not None:
                raise ModellingError("Setting field {} of ModelJob. Field has already been set.".format(k))

            setattr(self, k, v)

        # Default time series values
        self.timesignals = {}
        for series in time_signal.signal_types:
            self.timesignals[series] = None

        if time_series:
            for series, values in time_series.iteritems():
                if series != values.name:
                    raise ModellingError("Time signal {} mislabelled as {}".format(values.name, series))
                self.timesignals[series] = values

        # n.b. We do NOT call check_job here. It is perfectly reasonably for the required data to come from
        #      multiple sources, and be merged in. Or added in with regression processes. check_job() should be
        #      called immediately before the first time the ModelJob is to be USED where validity would be required.

    def merge(self, other):
        """
        Merge together two ModelJobs that are (trying) to represent the same actual job. They likely come from
        separate sets of profiling.
        """
        assert self.label == other.label

        #############################################
        # TODO: CRITICAL. Currently throwing data away!!!
        ##############################################

        self.merge_start_times(other)

        self.merge_time_signals(other)

    def merge_start_times(self, other):
        """
        Pick the best start time from each of the sources.

        i) Start times from scheduler data take precidence
        ii) Otherwise, pick a non-None value if it exists
        iii) If multiple values relevant, pick the earliest one
        """
        # Nothing to merge if nothing meaningful in other.
        if other.time_start:

            if other.scheduler_timing:
                if self.scheduler_timing:
                    # n.b. We assert that if scheduler timing is in use, then we MUST have a time_start
                    assert self.time_start is not None
                    self.time_start = min(self.time_start, other.time_start)
                else:
                    # n.b. We assert that if scheduler timing is in use, then we MUST have a time_start
                    self.time_start = other.time_start

                # The merged job now contains scheduler timing.
                self.scheduler_timing = True

            elif not self.scheduler_timing:

                if self.time_start is not None:
                    self.time_start = min(self.time_start, other.time_start)
                else:
                    self.time_start = other.time_start

    def merge_time_signals(self, other):
        """
        Combine the time signals from multiple sources. Currently we only support one non-zero time signal
        for each signal type
        """
        for ts_name in time_signal.signal_types:

            # There is nothing to copy in, if the other time signal is not valid...
            other_valid = other.timesignals[ts_name] is not None and other.timesignals[ts_name].sum != 0
            self_valid = self.timesignals[ts_name] is not None and self.timesignals[ts_name].sum != 0

            # Validity checks
            if self_valid and ts_name != self.timesignals[ts_name].name:
                raise ModellingError("Time signal {} mislabelled as {}".format(self.timesignals[ts_name].name, ts_name))

            # There is only merging to do if the data is present in the _other_ job.
            if other_valid:

                # Validity checks
                if ts_name != other.timesignals[ts_name].name:
                    raise ModellingError(
                        "Time signal {} mislabelled as {}".format(other.timesignals[ts_name].name, ts_name))

                if self_valid:
                    raise ModellingError("Valid timeseries in both model jobs for {}: {}".format(
                        ts_name, self.label
                    ))
                else:
                    self.timesignals[ts_name] = other.timesignals[ts_name]

    def verbose_description(self):
        """
        A verbose output for the modelled workload
        """

        output = None
        for ts_name, ts in self.timesignals.iteritems():
            if ts is not None and ts.sum != 0:
                if output is None:
                    output = "Num samples: \t{}\n".format(len(ts.xvalues))
                output += "{}: \t{}\n".format(ts_name, sum(ts.yvalues))

        output = "Start time: \t{}\n".format(format_seconds(self.time_start)) + (output or "")

        return output

    def check_job(self):
        """
        Some quick sanity checks
        """
        if not self.is_valid():
            raise KeyError("Job is missing field")

    def is_valid(self):
        """
        Return false if the job is not complete, and usable
        :return:
        """
        for field in self.required_fields:
            if getattr(self, field, None) is None:
                print_colour("red", "Job is incomplete. Missing field: {}".format(field))
                return False

        return True

    @property
    def job_impact_index(self):
        """
        This is a naive approach to determining a job impact factor. It is the sum of the integrals of
        all of the time series --- i.e. the more resources  job uses the higher its "impact.

        A side effect of this is that jobs without profiling data are weighted to zero.
        """
        index = 0
        for series in self.timesignals.values():
            if series is not None:
                index += np.trapz(abs(series.yvalues), series.xvalues)

        return index


class IngestedJob(object):

    required_fields = [
        'time_created',          ## From logs
        'time_queued',
        'time_eligible',
        'time_end',
        'time_start',
        'ncpus',
        'nnodes',
        'memory_kb',

        'group',
        'jobname',
        'user',
        'queue_type',

        'runtime',               ## Derived
        'time_start_0',
        'time_in_queue',

        'timesignals',           ## Additional,
        'job_impact_index_rel',
        '_job_impact_index',
    ]

    # from logs
    time_created = None
    time_queued = None
    time_eligible = None
    time_end = None
    time_start = None
    ncpus = None
    nnodes = None
    memory_kb = None
    filename = None

    # self.cpu_percent = None
    group = None
    jobname = None
    user = None
    queue_type = None

    # derived
    runtime = None
    time_start_0 = None
    time_in_queue = None
    idx_in_log = None

    # added
    job_impact_index_rel = None
    _job_impact_index = None

    def __init__(self, label=None, **kwargs):

        self.label = label
        self.timesignals = {}

        # Other parameters to update
        for key, value in kwargs.iteritems():
            if not hasattr(self, key):
                raise AttributeError("Attribute {} of {} is unknown".format(key, self.__class__.__name__))
            else:
                setattr(self, key, value)

    # aggregate time signals..
    def append_time_signal(self, time_signal_in):
        assert time_signal_in.name not in self.timesignals

        self.timesignals[time_signal_in.name] = time_signal_in

    # job impact index..
    @property
    def job_impact_index(self):

        if not self.timesignals:
            raise UserWarning('no timesignal found! => _impact_idx is set to zero')
        else:
            self._job_impact_index = 0
            for i_ts in self.timesignals:
                self._job_impact_index += np.trapz(abs(i_ts.yvalues), i_ts.xvalues)

        return self._job_impact_index

    # @job_impact_index.setter
    # def job_impact_index(self, value):
    #     self._job_impact_index = value

    def check_job(self):
        """
        Some quick sanity checks
        """
        for field in self.required_fields:
            if getattr(self, field, None) is None:
                raise UserWarning("job: {}, missing field: {}".format(self.jobname, field))
