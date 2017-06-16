# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
We need a way to convert a time series into the input for the synthetic app kernels
"""
from kronos.core.kronos_tools.print_colour import print_colour


class KernelBase(object):

    name = None
    signals = ()

    def __init__(self, timesignals, stretching_factor_dict=None, metrics_hard_limits=None):

        self.timesignals = timesignals
        self.stretching_factor_dict = stretching_factor_dict
        self.metrics_hard_limits = metrics_hard_limits

    @property
    def empty(self):
        """
        Return True if this kernel would do nothing (and can therefore be omitted from the list)
        """
        return all((ts.sum == 0 for ts in self.timesignals.values()))

    def extra_data(self, **kwargs):
        data = {'name': self.name}
        if kwargs:
            data.update(kwargs)
        return data

    def apply_hard_limits(self, time_signal_names, time_signals):
        """
        Apply hard limits if specified
        :param time_signal_names:
        :param time_signals:
        :return:
        """

        if self.metrics_hard_limits:
            for tt, ts_name in enumerate(time_signal_names):

                if self.metrics_hard_limits.get(ts_name):

                    if sum(time_signals[tt]) > self.metrics_hard_limits[ts_name]:
                        print_colour("orange", "applied hard limit to metric {} in kernel {}".format(ts_name, self.name),
                                     log_level="debug")
                        sc_ = self.metrics_hard_limits[ts_name] / float(sum(time_signals[tt]))
                        time_signals[tt] = [t * sc_ for t in time_signals[tt]]

        return time_signals

    def synapp_config(self, n_bins=None):
        """
        Return a synthetic app configuration. This can be overridden for more complex behaviour if required
        """

        # This code takes a list of signal-name and key-name pairs (in general these don't have to be
        # equal, but in all cases so far they are.
        #
        # i) Extract the data series associated with each of these
        # ii) Put that data into a sequence of dictionaries, with each element labelled with the appropriate key

        time_signal_names, key_names = zip(*self.signals)

        factors = {name: 1.0 for name in time_signal_names}
        factors.update(self.stretching_factor_dict or {})

        time_signals = [self.timesignals[ts_name].digitized(nbins=n_bins)[1] * factors[ts_name] for ts_name in time_signal_names]

        # apply hard limits if set by user..
        time_signals = self.apply_hard_limits(time_signal_names, time_signals)

        frames = [ { k: v for k, v in zip(key_names, ts_data) } for ts_data in zip(*time_signals) ]

        # Add any extra data, which is constant for all the elements.

        extra_data = self.extra_data()
        for frame in frames:

            # If the time series is empty, then mark it as such (so it can be excluded later)
            if all((val == 0.0 for val in frame.values())):
                frame['empty'] = True

            frame.update(extra_data)

        return frames


class CPUKernel(KernelBase):

    name = 'cpu'
    signals = (("flops", "flops"),)

    def synapp_config(self, n_bins=None):
        """
        Special case code: apply max value constrain.
        """
        data = super(CPUKernel, self).synapp_config(n_bins=n_bins)

        for d in data:

            if d['flops'] < 0.:
                raise ValueError('flops={} is < 0!'.format(d['flops']))

            if d['flops'] >= 0:
                d['flops'] = int(d['flops'])

        return data


class MPIKernel(KernelBase):

    name = 'mpi'
    signals = (
        ("n_collective", "n_collective"),
        ("kb_collective", "kb_collective"),
        ("n_pairwise", "n_pairwise"),
        ("kb_pairwise", "kb_pairwise"))

    def synapp_config(self,n_bins=None):
        """
        Special case code: We cannot have a zero number of actions for a non-zero amount of data.
        """
        data = super(MPIKernel, self).synapp_config(n_bins=n_bins)

        for d in data:

            if d['kb_collective'] < 0.:
                raise ValueError('kb_collective={} is < 0!'.format(d['kb_collective']))

            if d['n_collective'] < 0.:
                raise ValueError('n_collective={} is < 0!'.format(d['n_collective']))

            if d['kb_pairwise'] < 0.:
                raise ValueError('kb_pairwise={} is < 0!'.format(d['kb_pairwise']))

            if d['n_pairwise'] < 0.:
                raise ValueError('n_pairwise={} is < 0!'.format(d['n_pairwise']))

            if d['kb_collective'] >= 0:
                d['n_collective'] = max(1, int(d['n_collective']))

            if d['kb_pairwise'] >= 0:
                d['n_pairwise'] = max(1, int(d['n_pairwise']))

        return data


class FileReadKernel(KernelBase):

    name = 'file-read'
    signals = (
        ("kb_read", "kb_read"),
        ("n_read", "n_read")
    )

    def extra_data(self):
        return super(FileReadKernel, self).extra_data(mmap=True, invalidate=True)

    def synapp_config(self, n_bins=None):
        """
        Special case code: We cannot have a zero number of actions for a non-zero amount of data.
        """
        data = super(FileReadKernel, self).synapp_config(n_bins=n_bins)

        for d in data:

            if d['kb_read'] < 0.:
                raise ValueError('kb_read is < 0!'.format(d['kb_read']))

            if d['n_read'] < 0.:
                raise ValueError('n_read is < 0!'.format(d['n_read']))

            if d['kb_read'] > 0:
                d['n_read'] = max(1, int(d['n_read']))
                d['kb_read'] = max(1.0, d['kb_read'])

            if d['n_read'] > 0:
                d['n_read'] = max(1, int(d['n_read']))
                d['kb_read'] = max(1.0, d['kb_read'])

        return data


class FileWriteKernel(KernelBase):

    name = 'file-write'
    signals = (
        ("kb_write", "kb_write"),
        ("n_write", "n_write")
    )

    def extra_data(self):
        return super(FileWriteKernel, self).extra_data()

    def synapp_config(self, n_bins=None):
        """
        Special case code: We cannot have a zero amount of data for a non-zero number of writes,
        nor a zero amount of data for a non-zero number of writes
        """
        data = super(FileWriteKernel, self).synapp_config(n_bins=n_bins)

        for d in data:

            if d['n_write'] < 0.:
                raise ValueError('n_write={} is < 0!'.format(d['n_write']))
            if d['kb_write'] < 0.:
                raise ValueError('kb_write={} is < 0!'.format(d['kb_write']))

            if d['n_write'] > 0:
                d['n_write'] = max(1, int(d['n_write']))
                d['kb_write'] = max(1.0, d['kb_write'])

            if d['kb_write'] > 0:
                d['n_write'] = max(1, int(d['n_write']))
                d['kb_write'] = max(1.0, d['kb_write'])

            # set the number of files
            d['n_files'] = max(1, int(d['n_write']/100.0))

        return data


available_kernels = [
    CPUKernel,
    MPIKernel,
    FileReadKernel,
    FileWriteKernel
]
