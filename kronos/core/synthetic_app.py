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
from collections import Counter

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
        self._scaling_factors = None

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

    def total_metrics_dict(self, include_scaling_factors=False):

        if not self._scaling_factors:
            raise ConfigurationError("scaling factors not set!")

        tot = {}
        fact = 1.0  # default tuning factor
        # loop oer synthetic apps and return sums of time signals
        for app in self.app_list:

            for ts in app.timesignals.values():

                if include_scaling_factors and app._scaling_factors is not None:
                    fact = app._scaling_factors[ts.name]

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
    def total_metrics_apps(self, n_bins=None):
        """
        Calculates the metrics of the whole workload (as sums of the kernels metrics) on the fly
        :return:
        """

        # initialize the sums
        metrics_sums = Counter({k: 0. for k in time_signal_names})

        # Query kernel metrics
        for s_app in self.app_list:
            metrics_sums += Counter(s_app.total_export_metrics(n_bins=n_bins))

        return metrics_sums

    @property
    def scaling_factors(self):
        """
        Get the tuning factors
        :return:
        """
        return self._scaling_factors

    @scaling_factors.setter
    def scaling_factors(self, scaling_factors):
        """
        Set the tuning factor for all the applications
        """
        # NB each app receives a *copy* of the scaling_factors dictionary..
        self._scaling_factors = scaling_factors
        for app in self.app_list:
            app._scaling_factors = dict(scaling_factors)

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
        for k in time_signal_names:
            str_format = "%s: %.3f"
            print str_format % (k, self.total_metrics_dict()[k])
        print "----------------------------------------"

        print " ----- Actual Scaling factors: ---------"
        for k in time_signal_names:
            str_format = "%s: %f"
            print str_format % (k, self.total_metrics_apps[k]/float(self.total_metrics_dict()[k]))
        print "----------------------------------------"

        print "-------- Exported Metrics sums: --------"
        for k in time_signal_names:
            str_format = "%s: "+signal_types[k]['format']
            print str_format % (k, self.total_metrics_apps[k])
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
        self._scaling_factors = None

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

        kernels = []
        for kernel_type in app_kernels.available_kernels:

            if self._scaling_factors:
                kernel = kernel_type(self.timesignals, self._scaling_factors)
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

    def total_export_metrics(self, n_bins=None):
        """
        Calculates the sums of the exported metrics (so, including corrections from kernel discretization)
        :param n_bins:
        :return:
        """

        # initialize the sums
        metrics_sums = {k: 0. for k in time_signal_names}

        # Query kernel metrics
        frames_list = self.export(n_bins=n_bins, job_entry_only=True)['frames']
        for frame in frames_list:
            for ker in frame:
                for k in ker.keys():
                    if k in time_signal_names:
                        metrics_sums[k] += ker[k]

        return metrics_sums

