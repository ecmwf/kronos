# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import json
import pickle
import logging
from collections import Counter

from kronos_exceptions import ConfigurationError
from jobs import ModelJob
from kronos_executor import app_kernels
from kronos_executor.io_formats.schedule_format import ScheduleFormat
from kronos_executor.definitions import time_signal_names

logger = logging.getLogger(__name__)


class SyntheticWorkload(object):
    """
    This is a workload of SyntheticApp objects, described below.
    """
    def __init__(self, config, apps=None):

        self.config = config
        self.app_list = apps

        # scaling factors applied to TS
        self._scaling_factors = None

        # glob scaling factor applied to TS and ncpus
        self._glob_scaling_factor = None

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
            print "scaling factors not set!, defaulting to 1.0"
            self._scaling_factors = {k: 1.0 for k in time_signal_names}

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

    def total_metrics_apps(self, n_bins=None):
        """
        Calculates the metrics of the whole workload (as sums of the kernels metrics) on the fly
        :return:
        """

        if not n_bins:
            raise ConfigurationError("n_bins should be set!")

        # initialize the sums
        metrics_sums = Counter({k: 0. for k in time_signal_names})

        # Query kernel metrics
        for s_app in self.app_list:
            metrics_sums += Counter(s_app.total_export_metrics(n_bins=n_bins))

        return metrics_sums

    @property
    def scaling_factors(self):
        """
        Get the time-series scaling factors
        :return:
        """
        return self._scaling_factors

    @property
    def glob_scaling_factor(self):
        """
        Get the global scaling factor that gets
        applied to the time-series AND ncpu that the
        synthetic app will require
        :return:
        """
        return self._glob_scaling_factor

    @scaling_factors.setter
    def scaling_factors(self, scaling_factors):
        """
        Set the tuning factor for all the applications
        """
        # NB each app receives a *copy* of the scaling_factors dictionary..
        self._scaling_factors = scaling_factors
        for app in self.app_list:
            app._scaling_factors = dict(scaling_factors)

    @glob_scaling_factor.setter
    def glob_scaling_factor(self, glob_scaling_factor):
        """
        Set the tuning factor for all the applications
        """
        # NB each app receives a *copy* of the scaling_factors dictionary..
        self._glob_scaling_factor = glob_scaling_factor
        for app in self.app_list:
            app._glob_scaling_factor = glob_scaling_factor

    def export_pickle(self):
        """
        Export the synthetic workload to pickle file..
        :return:
        """
        with open(os.path.join(self.config.dir_output, 'sa_workload_pickle'), 'w') as f:

            pickle.dump(self, f)

            info_str = "Synthetic workload pickle file saved to {}".format(self.config.dir_output)
            logger.info(info_str)

    def max_sa_time_interval(self):
        """
        Calculate the maximum time delay for the synthetic apps
        :return:
        """

        return max(sa.time_start for sa in self.app_list)

    def export_kschedule(self, filename):
        """
        Write a KSchedule file that describes the synthetic time_schedule,
        this file can be given directly to the kronos_executor
        :return:
        """
        n_bins = self.config.model["schedule_exporting"]["synth_apps_n_bins"]
        trunc_pc = self.config.model["schedule_exporting"].get('truncate_at_percent')

        logger.info("Exporting {} synth-apps to KSchedule: {}".format(len(self.app_list), filename))

        ScheduleFormat.from_synthetic_workload(self,
                                               n_bins=n_bins,
                                               trunc_pc=trunc_pc
                                               ).write_filename(filename, indent=2)


class SyntheticApp(ModelJob):
    """
    A type of ModelJob which is used as the output stage (after modelling, scaling, etc.).
    There is a one-to-one mapping between these SyntheticApp classes and execution runs of
    the synthetic application (coordinator).
    """
    def __init__(self, job_name="unknown",
                 time_signals=None,
                 metrics_hard_limits=None, **kwargs):

        super(SyntheticApp, self).__init__(
            timesignals=time_signals,
            duration=None,  # Duration is not specified for a Synthetic App - that is an output
            **kwargs
        )

        self.job_name = job_name

        # scaling factors applied to the time series of this app
        self._scaling_factors = None

        # global scaling factor that gets applied to the time-series of
        # this app plut the ncpus that this app will require..
        self._glob_scaling_factor = None

        # synthetic app hard limits..
        self.metrics_hard_limits = metrics_hard_limits

    def export(self, filename=None, n_bins=None, trunc_pc=None, job_entry_only=False):
        """
        Produce a json file suitable to control a synthetic application (coordinator)
        """

        frames = self.frame_data(n_bins, trunc_pc)

        # decide the n_cpus when exporting this synapp (it depends also
        # on the global scaling factor)
        if self._glob_scaling_factor:
            n_cpus = max(1, int(self.ncpus * self._glob_scaling_factor))
        else:
            n_cpus = max(1, int(self.ncpus))

        job_entry = {
            'num_procs': n_cpus,
            'start_delay': self.start_delay if self.start_delay else 0,
            'depends': self.depends,
            'frames': frames,
            'metadata': {
                'job_name': self.job_name if self.job_name else "generic-job",
                'workload_name': self.label if self.label else "generic-workload",
            }
        }

        # before exporting, make sure that ncpu>=2 if there are MPI instructions
        for frame in frames:
            for ker in frame:
                if ker["name"] == "mpi":
                    job_entry["num_procs"] = max(2, job_entry["num_procs"])
                    break

        # if job_entry_only, export dictionary data only
        if job_entry_only:
            return job_entry
        else:
            if filename is None:
                err_str = "trying to export synthetic apps to {} file".format(filename)
                raise ConfigurationError(err_str)
            else:
                with open(filename, 'wa') as f:
                    json.dump(job_entry, f,
                              ensure_ascii=True,
                              sort_keys=True,
                              indent=4,
                              separators=(',', ': ')
                              )

    def frame_data(self, n_bins=None, trunc_pc=None):
        """
        Write data in each frame of the synthetic apps
        :param n_bins: N bins for kernels
        :param trunc_pc: truncation percentage
        :return:
        """

        kernels = []
        for kernel_type in app_kernels.available_kernels:

            kernel = kernel_type(self.timesignals,
                                 self._scaling_factors,
                                 self.metrics_hard_limits)

            # if these
            if not kernel.empty:
                kernels.append(kernel.synapp_config(n_bins=n_bins, trunc_pc=trunc_pc))

        # check if all the kernels have the same length
        if not all(len(i) == len(kernels[0]) for i in kernels):
            raise ConfigurationError("something is wrong, kernels should have the same length!")

        # Instead of a list of kernels, each of which contains a time series,
        # we want a time series each of which contains a list of kernels =>
        # Do the inversion!
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

