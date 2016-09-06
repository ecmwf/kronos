from jobs import ModelJob
import json
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

    def export(self, nbins, dir_output=None, stretching_factors=None):

        # A default dir, that can be overridden
        dir_output = dir_output or self.config.dir_output

        # Ensure that the synthetic applications are sorted by start time
        sorted_apps = sorted(self.app_list, key=lambda a: a.time_start)

        print "Exporting {} synthetic applications to: {}".format(len(sorted_apps), dir_output)

        if stretching_factors:
            for i, app in enumerate(sorted_apps):
                app.export(os.path.join(dir_output, "job-{}.json".format(i)), nbins, stretching_factors)
        else:
            for i, app in enumerate(sorted_apps):
                app.export(os.path.join(dir_output, "job-{}.json".format(i)), nbins)

    @property
    def total_metrics_dict(self):

        tot = {}

        # loop oer synthetic apps and return sums of time signals
        for app in self.app_list:
            for ts in app.timesignals.values():
                if ts.name in tot:
                    tot[ts.name] += ts.sum
                else:
                    tot[ts.name] = ts.sum

        return tot

    def set_tuning_factors(self, tuning_factor):
        """ set the stretching factor for all the applications """
        # loop oer synthetic apps and return sums of time signals
        for app in self.app_list:
            app.tuning_factor = tuning_factor

    def print_metrics_sums(self):
        for metric in self.total_metrics_dict.keys():
            print "[synth apps]: sum of {} = {}".format(metric, self.total_metrics_dict[metric])


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

        # this additional "scaling factor" is generated during the workload "tuning" phase
        # and acts on the time series when the synthetic apps are exported
        self.tuning_factor = None

    def export(self, filename, nbins):
        """
        Produce a json file suitable to control a synthetic application (coordinator)
        """

        frames = self.frame_data(nbins)

        job_entry = {
            'num_procs': self.ncpus,
            'start_delay': self.time_start,
            'frames': frames,
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

