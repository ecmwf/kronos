from exceptions_iows import ConfigurationError
from jobs import ModelJob
import json
import os
import pickle

import app_kernels
from kronos_tools.print_colour import print_colour
from ksf_handler import KSFFileHandler
from time_signal import TimeSignal
from time_signal import signal_types
import numpy as np


class SyntheticWorkload(object):
    """
    This is a workload of SyntheticApp objects, described below.
    """
    def __init__(self, config, apps=None):
        self.config = config
        self.app_list = apps
        self.tuning_factor = None

    def __unicode__(self):
        return "SyntheticWorkload - {} jobs".format(len(self.app_list))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def verbose_description(self):
        """
        A verbose output for the modelled workload
        """
        output = "=============================================\n"
        output += "Verbose (synthetic) workload description: {}\n".format(self)

        for i, app in enumerate(self.app_list):
            output += '--\nApp {}:\n'.format(i)
            output += app.verbose_description()

        output += "=============================================\n"
        return output

    def total_metrics_dict(self, include_tuning_factors=False):

        if not self.tuning_factor:
            raise ConfigurationError("tuning factors not set!")

        tot = {}
        fact = 1.0  # default tuning factor
        # loop oer synthetic apps and return sums of time signals
        for app in self.app_list:

            for ts in app.timesignals.values():

                if include_tuning_factors and app.tuning_factor is not None:
                    fact = app.tuning_factor[ts.name]

                if ts.name in tot:
                    if ts is not None:
                        if ts.sum is not None:
                            tot[ts.name] += ts.sum * fact
                else:
                    if ts is not None:
                        if ts.sum is not None:
                            tot[ts.name] = ts.sum * fact
        return tot

    def set_tuning_factors(self, tuning_factor):
        """
        Set the tuning factor for all the applications
        """
        # NB each app receives a *copy* of the tuning_factor dictionary..
        self.tuning_factor = tuning_factor
        for app in self.app_list:
            app.tuning_factor = dict(tuning_factor)

    def get_tuning_factors(self):
        """
        Get the tuning factors
        :return:
        """
        return self.tuning_factor

    def export_synth_apps(self, nbins, dir_output=None):
        """
        Export the synthetic apps to json files
        :param nbins:
        :param dir_output:
        :return:
        """

        # A default dir, that can be overridden
        dir_output = dir_output or self.config.dir_output

        # Ensure that the synthetic applications are sorted by start time
        sorted_apps = sorted(self.app_list, key=lambda a: a.time_start)

        print "Exporting {} synthetic applications to: {}".format(len(sorted_apps), dir_output)

        for i, app in enumerate(sorted_apps):
            app.export(os.path.join(dir_output, "job-{}.json".format(i)), nbins)

    def export_pickle(self):
        """
        Export the synthetic workload to pickle file..
        :return:
        """
        with open(os.path.join(self.config.dir_output, 'sa_workload_pickle'), 'w') as f:
            pickle.dump(self, f)
            print_colour("green", "synthetic workload pickle file saved to {}".format(self.config.dir_output))

    def export_ksf(self, nbins, filename):
        """
        Write a KSF file that describes the synthetic schedule,
        this file can be given directly to the executor
        :return:
        """
        ksf_file = KSFFileHandler().from_synthetic_workload(self)
        ksf_file.export(nbins, filename)


class SyntheticApp(ModelJob):
    """
    A type of ModelJob which is used as the output stage (after modelling, scaling, etc.). There is a one-to-one
    mapping between these SyntheticApp classes and execution runs of the synthetic application (coordinator).
    """
    def __init__(self, job_name=None, time_signals=None, **kwargs):

        super(SyntheticApp, self).__init__(
            time_series=time_signals,
            duration=None,  # Duration is not specified for a Synthetic App - that is an output
            **kwargs
        )

        self.job_name = job_name
        # self.timesignals = {k:v for k,v in self.timesignals.items() if v is not None}

        # this additional "scaling factor" is generated during the workload "tuning" phase
        # and acts on the time series when the synthetic apps are exported
        self.tuning_factor = None
        self.set_ts_defaults()

    def set_ts_defaults(self):
        """ Set default values for time-series that are not included
        in the model job from which this synthetic app has been derived """
        if self.timesignals:
            for ts_name in signal_types:
                if self.timesignals[ts_name] is None:
                    ts = TimeSignal(ts_name)
                    ts = ts.from_values(ts_name, np.zeros(10), np.zeros(10), base_signal_name=None, durations=np.zeros(10)+0.01)
                    self.timesignals[ts_name] = ts

    def export(self, filename, n_bins, job_entry_only=False):
        """
        Produce a json file suitable to control a synthetic application (coordinator)
        """

        frames = self.frame_data(n_bins)

        job_entry = {
            'num_procs': self.ncpus,
            'start_delay': self.time_start,
            'frames': frames,
            'metadata': {
                'job_name': self.job_name
            }
        }

        if job_entry_only:
            return job_entry
        else:
            with open(filename, 'wa') as f:
                json.dump(job_entry, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

    def frame_data(self, n_bins):

        # generate kernes from non-null timesignals only..
        self.set_ts_defaults()

        # nonnull_timesignals = {k: v for k, v in self.timesignals.items() if (v.xvalues is not None) and (v.yvalues is not None)}
        # print nonnull_timesignals

        # (re-)digitize all the time series
        for ts_name, ts in self.timesignals.iteritems():
            ts.digitize(n_bins)

        kernels = []
        for kernel_type in app_kernels.available_kernels:

            if self.tuning_factor:
                kernel = kernel_type(self.timesignals, self.tuning_factor)
            else:
                kernel = kernel_type(self.timesignals)

            if not kernel.empty:
                kernels.append(kernel.synapp_config())

        # Instead of a list of kernels, each of which contains a time series, we want a time series each of
        # which contains a list of kernels. Do the inversion!
        frames = zip(*kernels)

        # Filter out empty elements from the list of kernels
        frames = [
            [kern for kern in frame if not kern.get("empty", False)]
            for frame in frames
        ]

        return frames

    # def stretch_time_series(self, st_dict):
    #
    #     """ rescale all the time series.. """
    #
    #     for ts_name, ts in self.timesignals.iteritems():
    #         ts.stretch_values(st_dict[ts_name])

