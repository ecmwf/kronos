import numpy as np
import time_signal

from exceptions_iows import ModellingError, ConfigurationError
from kronos_tools.merge import max_not_none, min_not_none
from kronos_tools.print_colour import print_colour
from kronos_tools.time_format import format_seconds
from time_signal import TimeSignal


class ModelJob(object):
    """
    A model job is as fully specified, abstract job, which is output from the data ingestion
    """

    # --------- absolutely required fields --------------
    job_name = None
    user_name = None
    cmd_str = None
    queue_name = None
    time_queued = None
    time_start = None
    duration = None
    ncpus = None
    nnodes = None
    stdout = None
    label = None  # placeholder for user defined labels..

    # -------------- optional fields ------------------
    # Times may come from the scheduler (+ associated systems), or from other profiling sources. Those from the
    # scheduler (n.b. None works as False)
    scheduler_timing = None
    # label = None

    timesignals = None

    # Does what it says on the tin
    required_fields = [
        'time_start',
        'duration',
        'ncpus',
        'nnodes'
    ]

    def __init__(self, timesignals=None, **kwargs):
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

        if timesignals:
            for series, values in timesignals.iteritems():

                # if this timeseries is available then get it into the modeljob timeseries..
                if values:
                    # this assumes that all the values are timeseries BUT they can be None..
                    if series != values.name:
                        raise ModellingError("Time signal {} mislabelled as {}".format(values.name, series))
                    self.timesignals[series] = values

        # n.b. We do NOT call check_job here. It is perfectly reasonably for the required data to come from
        #      multiple sources, and be merged in. Or added in with regression processes. check_job() should be
        #      called immediately before the first time the ModelJob is to be USED where validity would be required.

    def merge(self, other, force=False):
        """
        Merge together two ModelJobs that are (trying) to represent the same actual job. They likely come from
        separate sets of profiling.
        """
        if not force:
            assert self.label == other.label

        # Not all profiled components of a job will use all the CPUs. Catch the highest.
        self.ncpus = max_not_none(self.ncpus, other.ncpus)
        self.nnodes = max_not_none(self.nnodes, other.nnodes)

        self.merge_start_times(other)
        self.merge_durations(other)
        self.scheduler_timing = self.scheduler_timing or other.scheduler_timing

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

            elif not self.scheduler_timing:

                self.time_start = min_not_none(self.time_start, other.time_start)

    def merge_durations(self, other):
        """
        Pick the best durations from each of the sources.

        i) Durations from scheduler data take precedence
        ii) Otherwise, pick a non-None value if it exists
        iii) If multiple values relevant, pick the earliest one
        """
        # Nothing to merge if nothing meaningful in other.
        if other.duration:

            if other.scheduler_timing:
                if self.scheduler_timing:
                    # n.b. We assert that if scheduler timing is in use, then we MUST have a time_start
                    assert self.time_start is not None
                    self.duration = max(self.duration, other.duration)
                else:
                    # n.b. We assert that if scheduler timing is in use, then we MUST have a time_start
                    self.duration = other.duration

            elif not self.scheduler_timing:

                self.duration = max_not_none(self.duration, other.duration)

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
                    if self.timesignals[ts_name].priority >= other.timesignals[ts_name].priority:
                        print_colour("orange", "Conflict for time-series: {}, original retained".format(ts_name))
                    else:
                        print_colour("orange", "Conflict for time-series: {}, other job retained".format(ts_name))
                        self.timesignals[ts_name] = other.timesignals[ts_name]

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
            raise KeyError("Job {} is missing field".format(self.job_name))

    def is_valid(self):
        """
        Return false if the job is not complete, and usable
        :return:
        """
        for field in self.required_fields:
            if getattr(self, field, None) is None:
                print_colour("red", "Job is incomplete. Missing field: {}".format(field))
                return False

        # check that all the timesignals are not null
        if not all(self.timesignals.values()):
            null_ts = [k for k,v in self.timesignals.items() if not v]
            print_colour("red", " job [{}] with label [{}] has null timesignals: {}".format(self.job_name,
                                                                                            self.label,
                                                                                            null_ts))
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

    def ts_to_vector(self, n_ts_bins):
        """
        Create vector of values from time signals
        :param n_ts_bins:
        :return:
        """

        non_value_flag = -1

        ts_vector = []
        for tsk in time_signal.signal_types.keys():
            ts = self.timesignals[tsk]
            if ts is not None:
                xvals, yvals = ts.digitized(n_ts_bins)
                ts_vector.extend(yvals)
            else:
                # print "none yvalues..."
                ts_vector.extend([non_value_flag for vv in range(0, n_ts_bins)])

        return np.asarray(ts_vector)

    def vector_to_ts(self, ts_vector, priority, idx_map=None, n_bins=None):
        """
        Apply a vector to the time-signals
        :param ts_vector:
        :param idx_map:
        :return:
        """

        # check that when the vector does not contain all the values, also the n_bins for each ts is provided
        if idx_map and not n_bins:
            raise ConfigurationError("the RS results are mapped to N_columns < N_columns_tot => n_bins is needed!")

        if not idx_map and n_bins:
            raise ConfigurationError("n_bins need to be specified only for mapped cases")

        if self.time_start is None:
            raise ConfigurationError("job start-time is needed to accept time-series")

        if self.duration is None:
            raise ConfigurationError("job duration is needed to accept time-series")

        if not idx_map:

            # case in which all the elements are filled up
            split_values = np.split(ts_vector, len(time_signal.signal_types.keys()))
            for tt, ts in enumerate(time_signal.signal_types.keys()):
                y_values = split_values[tt]
                x_values = np.linspace(0.0, self.duration, len(y_values))

                if not self.timesignals[ts]:
                    self.timesignals[ts] = TimeSignal(ts).from_values(ts, x_values, y_values, priority=priority)
                elif self.timesignals[ts].priority <= priority:
                    self.timesignals[ts] = TimeSignal(ts).from_values(ts, x_values, y_values, priority=priority)

        else:

            # case for which only some columns are filled up (therefore the mapping)
            row = np.zeros(len(time_signal.signal_types.keys())*n_bins)
            for tt, ts in enumerate(ts_vector):
                row[idx_map[tt]] = ts

            # then re-call the same function without mapping
            self.vector_to_ts(row, priority)


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
    stdout = None

    # self.cpu_percent = None
    group = None
    jobname = None
    user = None
    queue_type = None
    cmd_str = None

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


def concatenate_modeljobs(cat_job_label, job_list):
    """
    Interlaces (or concatenates) a list of jobs into one single job..
    the job time series will be interlaced according to their respective timestamps..
    This function is used to generate one single model job that results to the same "workload" as the input group of jobs
    :param   cat_job_label: name of concatenated job
             job_list: list of jobs to concatenate
    :return: A ModelJob
    """

    # 2) find start-time and end-time
    cat_start_time = min([job.time_start for job in job_list])
    cat_end_time = max([job.time_start+job.duration for job in job_list])

    # 3) find overall duration
    cat_duration = cat_end_time - cat_start_time

    # 4) interlace time-series
    cat_time_series = {}
    for ts_type in time_signal.signal_types:

        cat_xvalues = []
        cat_yvalues = []
        cat_durations = []

        # loop over jobs
        for job in job_list:
            if ts_type in job.timesignals.keys() and job.timesignals[ts_type] is not None:

                # add xvalues (in absolute value) and yvalues
                cat_xvalues.extend(np.asarray(job.timesignals[ts_type].xvalues) + job.time_start)
                cat_yvalues.extend(np.asarray(job.timesignals[ts_type].yvalues))

                # add durations only if available, otherwise set them to zero..
                if job.timesignals[ts_type].durations is not None:
                    cat_durations.extend(job.timesignals[ts_type].durations)
                else:
                    cat_durations.extend(np.zeros(len(job.timesignals[ts_type].xvalues)))

        if cat_xvalues:
            # reset the initial time to zero..
            cat_xvalues = [x-cat_start_time for x in cat_xvalues]

            # sort values as time sequence..
            cat_vals = zip(cat_xvalues, cat_yvalues, cat_durations)
            cat_vals.sort(key=lambda x: x[0], reverse=False)
            xvalues, yvalues, durations = zip(*cat_vals)

            # build the concatenated time signal..
            cat_time_series[ts_type] = TimeSignal(
                                             ts_type,
                                             base_signal_name=ts_type,
                                             durations=durations,
                                             xvalues=xvalues,
                                             yvalues=yvalues
                                             )
    print "job {} created".format(cat_job_label)

    # TODO: make a decision on what nnodes and nproc to choose..
    return ModelJob(
        time_start=cat_start_time,
        duration=cat_duration,
        ncpus=2,
        nnodes=1,
        timesignals=cat_time_series,
        label=cat_job_label
    )
