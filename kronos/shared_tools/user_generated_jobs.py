# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy

import numpy as np

from kronos.core.jobs import ModelJob
from kronos.core.time_signal.definitions import time_signal_names
from kronos.core.time_signal.time_signal import TimeSignal


class UserGeneratedJob(object):
    """
    A user-generated job (mainly a name and a dictionary
    of generated timesignals)
    """

    # default values of n_proc and n_nodes
    n_procs = 1
    n_nodes = 1

    def __init__(self, name=None, timesignals=None, ts_scales=None):

        self.name = name
        self.timesignals = timesignals if timesignals else {}
        self.ts_scales = ts_scales if ts_scales else {}

        # scaleup factors
        scale_up_factors = {ts_name: 1.0 for ts_name in time_signal_names}
        scale_up_factors.update(self.ts_scales)

    def add_timesignal(self, timesignal):
        """
        Append another timesignal to the job
        :param timesignal:
        :return:
        """
        self.timesignals.update({timesignal.name: timesignal})

    @staticmethod
    def proto_signals(ts_len=5):
        """
        Return prototype signals
        :param ts_len:
        :return:
        """

        # prototype signals to chose from.
        # Each time-signal will stem from one of the
        # prototype signals of this list
        return [

            # sin with 5 full cycles
            np.abs(np.sin(np.linspace(0, 5 * 2 * np.pi, ts_len))),

            # cos with 3 full cycles
            np.abs(np.cos(np.linspace(0, 3 * 2 * np.pi, ts_len))),

            # central "dirac"-type of signal
            np.asarray([val if ts_len/3 < val < 2*ts_len/3 else 0. for val in range(ts_len)])

        ]

    @staticmethod
    def from_random_proto_signals(job_name=None, ts_scales=None, ts_len=10):
        """
        Generate a job from randomly chosen signals (from signal prototypes..)
        :param job_name:
        :param ts_scales:
        :param ts_len:
        :return:
        """

        ts_scales = ts_scales if ts_scales else {}

        proto_signals = UserGeneratedJob.proto_signals(ts_len=ts_len)

        timesignals = {}

        # Add all the time-signals
        for ts_name in time_signal_names:

            xvalues = np.arange(ts_len)

            # toss a coin to decide which signal type to choose
            proto_signal_idx = np.random.randint(len(proto_signals))
            yvalues = np.asarray(proto_signals[proto_signal_idx])

            if ts_scales:
                yvalues = yvalues * ts_scales[ts_name]

            timesignals[ts_name] = TimeSignal.from_values(ts_name,
                                                          xvalues,
                                                          yvalues,
                                                          priority=10)

        return UserGeneratedJob(name=job_name,
                                timesignals=timesignals,
                                ts_scales=ts_scales)

    def apply_ts_probability(self, prob):
        """
        Toss a coin for each timesignal and decide whether to keep it or drop it
        if a time-signal is dropped, replace its values with -1
        :return:
        """

        pruned_time_signals = {}
        for tsk, tsv in self.timesignals.iteritems():
            pruned_time_signals[tsk] = copy.deepcopy(tsv)

            # if the time-signal is not available, repalce all its values with -1
            if np.random.rand() > prob:
                pruned_time_signals[tsk].yvalues *= 0.0
                pruned_time_signals[tsk].yvalues += -1.0

        self.timesignals = pruned_time_signals

    def max_duration(self):
        """
        Max signal duration
        :return:
        """

        return max([max(tsv.xvalues) for tsk, tsv in self.timesignals.iteritems()])

    def model_job(self):
        """
        Return a model job from this job
        :return:
        """

        return ModelJob(
            time_start=0,
            duration=self.max_duration(),
            ncpus=self.n_procs,
            nnodes=self.n_nodes,
            timesignals=self.timesignals,
            label=self.name
        )

    def get_time_signal_length(self):
        """
        Return the length of the time-signals
        :return:
        """

        # NOTE: User-generated job time-signals all have the same length!
        return len(self.timesignals.values()[0].yvalues)


class UserGeneratedJobSet(object):
    """
    A Set of usergeneratedjobs, it coordinates the generation
    of UserGeneratedJobs (e.g. from a few prototype jobs)
    """

    def __init__(self, jobs=None, job_labels=None):

        # the core jobs
        self.jobs = jobs

        # in case the jobs are labelled..
        self.job_labels = job_labels

    @staticmethod
    def from_prototype_jobs(proto_jobs, n_jobs=1, ts_probability=1.0):

        # randomly choose a proto job and copy from it
        generated_jobs = []
        job_labels = []
        for job in range(n_jobs):

            proto_job_idx = np.random.randint(len(proto_jobs))

            job_labels.append(proto_job_idx)
            new_job = copy.deepcopy(proto_jobs[proto_job_idx])
            new_job.apply_ts_probability(prob=ts_probability)
            generated_jobs.append(new_job)

        return UserGeneratedJobSet(jobs=generated_jobs,
                                   job_labels=job_labels)

    def model_jobs(self):
        """
        Returns model jobs
        :return:
        """

        for job in self.jobs:
            yield job.model_job()

    def write_job_class_labels(self, filename):
        """
        Write file class labels to output file
        :param filename:
        :return:
        """

        with open(filename, "w") as f:
            f.write("\n".join([str(l) for l in self.job_labels]))

    def write_input_matrix_to_file(self, filename):
        """
        Output input matrix to file
        :param filename:
        :return:
        """

        # NOTE: all the signals of all the jobs in the set have the same length
        ts_len = self.jobs[0].get_time_signal_length()

        # Write out the prototype classes of the jobs into a csv file (for validation purposes..)
        with open(filename, "w") as f:

            # header row
            f.write(" ".join(["{}-{}".format(ts, str(val))
                              for ts in time_signal_names
                              for val in range(ts_len)]))

            f.write("\n")

            # entry
            f.write("\n".join([
                " ".join([str(y) for tsk in time_signal_names for y in j.timesignals[tsk].yvalues])
                for j in self.jobs]))


def write_job_prototype_classes(file_output_name, model_job_classes):
    """
    Write job class prototypes to output file
    :param file_output_name:
    :param model_job_classes:
    :return:
    """

    assert len(model_job_classes) >= 1

    ts_len = model_job_classes[0].get_time_signal_length()

    # Write out the prototype classes of the jobs into a csv file (for validation purposes..)
    with open(file_output_name, "w") as f:

        # header row
        f.write(" ".join(["{}-{}".format(ts, str(val))
                          for ts in time_signal_names
                          for val in range(ts_len)]))

        f.write("\n")

        # entry
        f.write("\n".join([
            " ".join([str(y) for tsk in time_signal_names for y in j.timesignals[tsk].yvalues])
            for j in model_job_classes]))