import time_signal
from jobs import ModelJob
import json
import sys
import os

import app_kernels


class SyntheticWorkload(object):
    """
    This is a workload of SyntheticApp objects, described below.
    """
    def __init__(self, config, apps=None):
        self.config = config
        self.app_list = apps

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


    def export(self, nbins, dir_output=None):

        # A default dir, that can be overridden
        dir_output = dir_output or self.config.dir_output

        # Ensure that the synthetic applications are sorted by start time
        sorted_apps = sorted(self.app_list, key=lambda a: a.time_start)

        print "Exporting {} synthetic applications to: {}".format(len(sorted_apps), dir_output)

        for i, app in enumerate(sorted_apps):
            app.export(os.path.join(dir_output, "input{}.json".format(i)), nbins)

        sys.exit(-1)


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

    def export(self, filename, nbins):
        """
        Produce a json file suitable to control a synthetic application (coordinator)
        """
        job_entry = {
            'num_procs': self.ncpus,
            'start_delay': self.time_start,
            'frames': self.frame_data(nbins),
            'metadata': {
                'job_name': self.job_name
            }
        }

        with open(filename, 'w') as f:
            json.dump(job_entry, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

    def frame_data(self, nbins):
        """
        Return the cofiguration required for the synthetic app kernels
        """
        # (re-)digitize all the time series
        for ts_name, ts in self.timesignals.iteritems():
            ts.digitize(nbins)

        kernels = []
        for kernel_type in app_kernels.available_kernels:

            kernel = kernel_type(self.timesignals)
            if not kernel.empty:
                kernels.append(kernel.synapp_config())

        # Instead of a list of kernels, each of which contains a time series, we want a time series each of
        # which contains a list of kernels. Do the inversion!
        frames = zip(*kernels)

        # Filter out empty elements from the list of kernels
        frames = [
            [kernel for kernel in frame if not kernel.get("empty", False)]
            for frame in frames
        ]

        return frames
