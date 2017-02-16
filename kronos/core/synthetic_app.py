# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import os
import pickle
import pprint

import app_kernels
from exceptions_iows import ConfigurationError
from jobs import ModelJob
from kronos.core.time_signal import time_signal_names, signal_types
from kronos.io.schedule_format import ScheduleFormat
from kronos_tools.print_colour import print_colour


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

    @property
    def total_metrics_apps(self):
        """
        Calculates the metrics of the whole workload (as sums of the kernels metrics) on the fly
        :return:
        """

        # initialize the sums
        metrics_sums = {k: 0. for k in time_signal_names}

        # Query kernel metrics
        for s_app in self.app_list:
            frames_list = s_app.export(job_entry_only=True)['frames']
            for frame in frames_list:
                for ker in frame:
                    for k in ker.keys():
                        if k in time_signal_names:
                            metrics_sums[k] += ker[k]

        # cast types appropriately
        for k in metrics_sums.keys():
            metrics_sums[k] = signal_types[k]['type'](metrics_sums[k])

        return metrics_sums

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

    def export_pickle(self):
        """
        Export the synthetic workload to pickle file..
        :return:
        """
        with open(os.path.join(self.config.dir_output, 'sa_workload_pickle'), 'w') as f:
            pickle.dump(self, f)
            print_colour("green", "synthetic workload pickle file saved to {}".format(self.config.dir_output))

    def export_ksf(self, filename):
        """
        Write a KSF file that describes the synthetic schedule,
        this file can be given directly to the executor
        :return:
        """

        print_colour("green", "Exporting {} synth-apps to KSF schedule: {}".format(len(self.app_list), filename))

        print "----- Metrics sums over workload: ------"
        for k, v in self.total_metrics_dict().iteritems():
            str_format = "%s: "+signal_types[k]['format']
            print str_format % (k, v)
        print "----------------------------------------"

        print " --------- Scaling factors: ------------"
        for k, v in self.tuning_factor.iteritems():
            str_format = "%s: %f"
            print str_format % (k, v)
        print "----------------------------------------"

        print "-------- Exported Metrics sums: --------"
        for k, v in self.total_metrics_apps.iteritems():
            str_format = "%s: "+signal_types[k]['format']
            print str_format % (k, v)
        print "----------------------------------------"

        ScheduleFormat.from_synthetic_workload(self).write_filename(filename)


class SyntheticApp(ModelJob):
    """
    A type of ModelJob which is used as the output stage (after modelling, scaling, etc.). There is a one-to-one
    mapping between these SyntheticApp classes and execution runs of the synthetic application (coordinator).
    """
    def __init__(self, job_name=None, time_signals=None, **kwargs):

        super(SyntheticApp, self).__init__(
            timesignals=time_signals,
            duration=None,  # Duration is not specified for a Synthetic App - that is an output
            **kwargs
        )

        self.job_name = job_name
        # self.timesignals = {k:v for k,v in self.timesignals.items() if v is not None}

        # this additional "scaling factor" is generated during the workload "tuning" phase
        # and acts on the time series when the synthetic apps are exported
        self.tuning_factor = None
        # self.set_ts_defaults()

    def export(self, filename=None, n_bins=None, job_entry_only=False):
        """
        Produce a json file suitable to control a synthetic application (coordinator)
        """

        frames = self.frame_data(n_bins)

        job_entry = {
            'num_procs': self.ncpus,
            'start_delay': self.time_start,
            'frames': frames,
            'metadata': {
                'job_name': self.job_name,
                'workload_name': self.label,
            }
        }

        # if job_entry_only, export dictionary data only
        if job_entry_only:
            return job_entry
        else:
            if filename is None:
                raise ConfigurationError("trying to export synthetic apps to {} file".format(filename))
            else:
                with open(filename, 'wa') as f:
                    json.dump(job_entry, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

    def frame_data(self, n_bins=None):
        """
        Write data in each frame of the synthetic apps
        :param n_bins:
        :return:
        """

        # # Discretise time signals (only if needed..)
        # if n_bins:
        #     for ts_name, ts in self.timesignals.iteritems():
        #         ts.digitize(n_bins)

        kernels = []
        for kernel_type in app_kernels.available_kernels:

            if self.tuning_factor:
                kernel = kernel_type(self.timesignals, self.tuning_factor)
            else:
                kernel = kernel_type(self.timesignals)

            if not kernel.empty:
                kernels.append(kernel.synapp_config(n_bins=n_bins))

        # Instead of a list of kernels, each of which contains a time series, we want a time series each of
        # which contains a list of kernels. Do the inversion!
        frames = zip(*kernels)

        # Filter out empty elements from the list of kernels
        frames = [
            [kern for kern in frame if not kern.get("empty", False)]
            for frame in frames
        ]

        # then filters out empty frames as well
        frames = [frame for frame in frames if frame]

        return frames

    # def stretch_time_series(self, st_dict):
    #
    #     """ rescale all the time series.. """
    #
    #     for ts_name, ts in self.timesignals.iteritems():
    #         ts.stretch_values(st_dict[ts_name])

