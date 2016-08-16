"""
We need a way to convert a time series into the input for the synthetic app kernels
"""


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

    def synapp_config(self):
        """
        Return a synthetic app configuration. This can be overridden for more complex behaviour if required
        """

        # This code takes a list of signal-name and key-name pairs (in general these don't have to be
        # equal, but in all cases so far they are.
        #
        # i) Extract the data series associated with each of these
        # ii) Put that data into a sequence of dictionaries, with each element labelled with the appropriate key

        time_signal_names, key_names = zip(*self.signals)

        # include a stretching factor is made available
        if self.stretching_factor_dict:
            time_signals = [self.timesignals[ts_name].yvalues_bins * self.stretching_factor_dict[ts_name] for ts_name in time_signal_names]
        else:
            time_signals = [self.timesignals[ts_name].yvalues_bins for ts_name in time_signal_names]

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


class MPIKernel(KernelBase):

    name = 'mpi'
    signals = (
        ("n_collective", "n_collective"),
        ("kb_collective", "kb_collective"),
        ("n_pairwise", "n_pairwise"),
        ("kb_pairwise", "kb_pairwise"))

    def synapp_config(self):
        """
        Special case code: We cannot have a zero number of actions for a non-zero amount of data.
        """
        data = super(MPIKernel, self).synapp_config()

        for d in data:
            if d['kb_collective'] > 0:
                d['n_collective'] = max(1, d['n_collective'])
            if d['kb_pairwise'] > 0:
                d['n_pairwise'] = max(1, d['n_pairwise'])

        return data


class FileReadKernel(KernelBase):

    name = 'file-read'
    signals = (
        ("kb_read", "kb_read"),
        ("n_read", "n_read")
    )

    def extra_data(self):
        return super(FileReadKernel, self).extra_data(mmap=False)

    def synapp_config(self):
        """
        Special case code: We cannot have a zero number of actions for a non-zero amount of data.
        """
        data = super(FileReadKernel, self).synapp_config()

        for d in data:
            if d['kb_read'] > 0:
                d['n_read'] = max(1, d['n_read'])

        return data


class FileWriteKernel(KernelBase):

    name = 'file-write'
    signals = (
        ("kb_write", "kb_write"),
        ("n_write", "n_write")
    )

    def extra_data(self):
        return super(FileWriteKernel, self).extra_data(mmap=False)

    def synapp_config(self):
        """
        Special case code: We cannot have a zero amount of data for a non-zero number of writes,
        nor a zero amount of data for a non-zero number of writes
        """
        data = super(FileWriteKernel, self).synapp_config()

        for d in data:
            if d['n_write'] > 0:
                d['kb_write'] = max(1, d['kb_write'])
            if d['kb_write'] > 0:
                d['n_write'] = max(1, d['n_write'])

        return data


available_kernels = [
    CPUKernel,
    MPIKernel,
    FileReadKernel,
    FileWriteKernel
]
