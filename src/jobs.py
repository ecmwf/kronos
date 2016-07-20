import numpy as np
import time_signal


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

        self.check_job()

    def verbose_description(self):
        """
        A verbose output for the modelled workload
        """
        output = None
        for ts_name, ts in self.timesignals.iteritems():
            if output is None:
                output = "Num samples: \t{}\n".format(len(ts.xvalues))
            output += "{}: \t{}\n".format(ts_name, sum(ts.yvalues))

        return output

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

    def __init__(self):

        # from logs
        self.time_created = None
        self.time_queued = None
        self.time_eligible = None
        self.time_end = None
        self.time_start = None
        self.ncpus = None
        self.nnodes = None
        self.memory_kb = None
        # self.cpu_percent = None
        self.group = None
        self.jobname = None
        self.user = None
        self.queue_type = None

        # derived
        self.runtime = None
        self.time_start_0 = None
        self.time_in_queue = None
        self.idx_in_log = None

        # added
        self.timesignals = {}
        self.job_impact_index_rel = None
        self._job_impact_index = None

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
