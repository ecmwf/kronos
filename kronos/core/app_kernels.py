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
from time_signal import signal_types

class KernelBase(object):

    name = None
    signals = ()

    def __init__(self, timesignals, stretching_factor_dict=None):
        self.timesignals = timesignals
        self.stretching_factor_dict = stretching_factor_dict

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

        # print time_signals
        # raw_input()

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
        data = super(CPUKernel, self).synapp_config(n_bins=None)

        for d in data:

            if d['flops'] > 0:
                d['flops'] = min(d['flops'], signal_types['flops']['max_value'])

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
        data = super(MPIKernel, self).synapp_config(n_bins=None)

        for d in data:

            if d['kb_collective'] >= 0:
                d['n_collective'] = min(max(1, int(d['n_collective'])), int(signal_types['n_collective']['max_value']))
                d['kb_collective'] = min(d['kb_collective'], signal_types['kb_collective']['max_value'])

            if d['kb_pairwise'] >= 0:
                d['n_pairwise'] = min(max(1, int(d['n_pairwise'])), int(signal_types['n_pairwise']['max_value']))
                d['kb_pairwise'] = min(d['kb_pairwise'], signal_types['kb_pairwise']['max_value'])

        return data


class FileReadKernel(KernelBase):

    name = 'file-read'
    signals = (
        ("kb_read", "kb_read"),
        ("n_read", "n_read")
    )

    def extra_data(self):
        return super(FileReadKernel, self).extra_data(mmap=False)

    def synapp_config(self, n_bins=None):
        """
        Special case code: We cannot have a zero number of actions for a non-zero amount of data.
        """
        data = super(FileReadKernel, self).synapp_config(n_bins=None)

        for d in data:
            if d['kb_read'] > 0:
                d['n_read'] = min(max(1, int(d['n_read'])), int(signal_types['n_read']['max_value']))
                d['kb_read'] = min(d['kb_read'], signal_types['kb_read']['max_value'])

        return data


class FileWriteKernel(KernelBase):

    name = 'file-write'
    signals = (
        ("kb_write", "kb_write"),
        ("n_write", "n_write")
    )

    def extra_data(self):
        return super(FileWriteKernel, self).extra_data(mmap=False)

    def synapp_config(self, n_bins=None):
        """
        Special case code: We cannot have a zero amount of data for a non-zero number of writes,
        nor a zero amount of data for a non-zero number of writes
        """
        data = super(FileWriteKernel, self).synapp_config(n_bins=None)

        for d in data:
            if d['n_write'] > 0:
                # d['kb_write'] = max(1, d['kb_write'])
                d['kb_write'] = min(max(1, d['kb_write']), signal_types['kb_write']['max_value'])
                d['n_write'] = min(int(d['n_write']), int(signal_types['n_write']['max_value']))
            if d['kb_write'] > 0:
                # d['n_write'] = max(1, d['n_write'])
                d['n_write'] = min(max(1, int(d['n_write'])), int(signal_types['n_write']['max_value']))
                d['kb_write'] = min(d['kb_write'], signal_types['kb_write']['max_value'])

        return data


available_kernels = [
    CPUKernel,
    MPIKernel,
    FileReadKernel,
    FileWriteKernel
]
