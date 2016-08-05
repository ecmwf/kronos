import numpy as np
import time_signal
from exceptions_iows import ModellingError


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
            assert hasattr(self, k) and getattr(self, k) is None

            setattr(self, k, v)

        # Default time series values
        self.timesignals = {}
        for series in time_signal.signal_types:
            self.timesignals[series] = None

        if time_series:
            for series, values in time_series.iteritems():
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

        for ts_name in time_signal.signal_types:

            # There is nothing to copy in, if the other time signal is not valid...
            other_valid = other.timesignals[ts_name] is not None and other.timesignals[ts_name].sum != 0
            if other_valid:

                self_valid = self.timesignals[ts_name] is not None and self.timesignals[ts_name].sum != 0
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

        return output or ""

    def check_job(self):
        """
        Some quick sanity checks
        """
        for field in self.required_fields:
            if getattr(self, field, None) is None:
                raise KeyError("job is missing field: {}".format(field))

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
