import numpy as np
import math
from kronos_tools import utils

# The availably types of time-series (or summed/averaged totals) data that we can use
signal_types = {

    # # CPU
    'flops':         {'type': float, 'category': 'cpu',        'behaviour': 'sum'},

    # (file) I/O
    'kb_read':       {'type': float, 'category': 'file-read',  'behaviour': 'sum'},
    'kb_write':      {'type': float, 'category': 'file-write', 'behaviour': 'sum'},
    'n_read':        {'type': int, 'category': 'file-read',  'behaviour': 'sum'},
    'n_write':       {'type': int, 'category': 'file-write', 'behaviour': 'sum'},

    # MPI activity
    'n_pairwise':    {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'kb_pairwise':   {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'n_collective':  {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'kb_collective': {'type': float, 'category': 'mpi',        'behaviour': 'sum'}
}


class TimeSignal(object):

    def __init__(self, name, **kwargs):

        self.name = name
        self.base_signal_name = kwargs.get('base_signal_name', self.name)
        if self.base_signal_name is None:
            self.base_signal_name = self.name
        assert self.base_signal_name in signal_types

        # vals in the time plane
        self.durations = kwargs.get('durations', None)
        self.xvalues = kwargs.get('xvalues', None)
        self.yvalues = kwargs.get('yvalues', None)

        # vals in freq domain
        self.freqs = kwargs.get('freqs', None)
        self.ampls = kwargs.get('ampls', None)
        self.phases = kwargs.get('phases', None)

        # digitized vals
        self.xvalues_bins = None
        self.yvalues_bins = None
        self.xedge_bins = None
        self.dx_bins = None

        # this index is used for ranking time-series priorities when merging jobs..
        # priority = None  -> signal not given
        # priority = 0     -> signal not reliable
        # 1<priority<9     -> signal partially reliable (e.g. ipm signals are less valuable than allinea signals, etc..)
        # priority = 10    -> totally reliable/valuable signal
        self.priority = kwargs.get('priority', None)

        # A quick sanity check (i.e. that we have used all the arguments)
        for k in kwargs:
            assert hasattr(self, k)

    def __unicode__(self):
        return "TimeSignal({})".format(self.name if self.name else "???")

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def ts_group(self):
        assert self.base_signal_name is not None and self.base_signal_name != ""
        return signal_types[self.base_signal_name]['category']

    @property
    def ts_type(self):
        assert self.base_signal_name is not None and self.base_signal_name != ""
        return signal_types[self.base_signal_name]['type']

    @property
    def digitization_key(self):
        assert self.base_signal_name is not None and self.base_signal_name != ""
        return signal_types[self.base_signal_name]['behaviour']

    # Create from frequency domain data
    @staticmethod
    def from_spectrum(name, time, freqs, ampls, phases, base_signal_name=None, priority=None):
        """
        A fatory method to construct TimeSignals from the correct elements
        :return: A newly constructed TimeSignal
        """
        return TimeSignal(
            name,
            base_signal_name=base_signal_name,
            xvalues=time,
            yvalues=abs(utils.freq_to_time(time - time[0], freqs, ampls, phases)),
            freqs=freqs,
            ampls=ampls,
            phases=phases,
            priority=priority
        )

    # Create from time-series data
    @staticmethod
    def from_values(name, xvals, yvals, base_signal_name=None, durations=None, priority=None):
        """
        A fatory method to construct TimeSignals from the correct elements
        :return: A newly constructed TimeSignal
        """
        return TimeSignal(
            name,
            base_signal_name=base_signal_name,
            durations=np.asarray(durations) if durations is not None else None,
            xvalues=np.asarray(xvals),
            yvalues=np.asarray(yvals),
            priority=priority
        )

    def digitized(self, nbins):
        """
        On-the-fly return digitized time series (rather than using
        """
        # Determine the bin boundaries
        xedge_bins = np.linspace(0.0, max(self.xvalues) + 1.0e-6, nbins + 1)

        # Return xvalues as the midpoints of the bins
        bins_delta = xedge_bins[1] - xedge_bins[0]
        xvalues = xedge_bins[1:] - (0.5 * bins_delta)

        # Split the data up amongst the bins
        # n.b. Returned indices will be >= 1, as 0 means to the left of the left-most edge.
        bin_indices = np.digitize(self.xvalues, xedge_bins)

        # Digitization key
        key = self.digitization_key

        yvalues = np.zeros(nbins)
        for i in range(nbins):
            if any(self.yvalues[bin_indices == i+1]):
                if key == 'mean':
                    val = self.yvalues[bin_indices == i+1].mean()
                elif key == 'sum':
                    val = self.yvalues[bin_indices == i+1].sum()
                else:
                    raise ValueError("Digitization key value not recognised: {}".format(key))

                yvalues[i] = val

        return xvalues, yvalues

    # calculate bins
    def digitize(self, n_bins, key=None):

        if self.durations is not None:
            return self.digitize_durations(n_bins, key)

        # Use a sensible default
        if key is None:
            key = self.digitization_key

        digitize_eps = 1.0e-6
        # self.xedge_bins = np.linspace(min(self.xvalues)-digitize_eps,
        #                               max(self.xvalues)+digitize_eps, n_bins + 1)
        # bins can be started from t=0.0
        self.xedge_bins = np.linspace(0.0, max(self.xvalues) + digitize_eps, n_bins + 1)
        self.dx_bins = self.xedge_bins[1] - self.xedge_bins[0]

        digitized_data = np.digitize(self.xvalues, self.xedge_bins)
        self.xvalues_bins = 0.5 * (self.xedge_bins[1:] + self.xedge_bins[:-1])

        self.yvalues_bins = np.zeros(len(self.xvalues_bins)) #+digitize_eps
        for i_bin in range(1, len(self.xvalues_bins)+1):
            if any(self.yvalues[digitized_data == i_bin]):
                if key == 'mean':
                    mean_val = self.yvalues[digitized_data == i_bin].mean()
                elif key == 'sum':
                    mean_val = self.yvalues[digitized_data == i_bin].sum()
                else:
                    raise ValueError('option not recognised!')
                self.yvalues_bins[i_bin-1] = mean_val

        self.yvalues_bins = np.asarray(self.yvalues_bins)
        self.yvalues_bins = self.yvalues_bins.astype(self.ts_type)

    def digitize_durations(self, nbins, key=None):

        assert self.durations is not None

        # Use a sensible default
        if key is None:
            key = self.digitization_key

        # Find out what the maximum time is
        self.xedge_bins = np.linspace(0.0, max(self.xvalues + self.durations) + 1.0e-6, nbins + 1)
        self.dx_bins = self.xedge_bins[1] - self.xedge_bins[0]
        self.xvalues_bins = 0.5 * (self.xedge_bins[1:] + self.xedge_bins[:-1])
        self.yvalues_bins = np.zeros(len(self.xvalues_bins))

        # Now we need to loop through to attribute the data to bins (as considering the durations, it may not be
        # one-to-one
        start_bins = np.digitize(self.xvalues, self.xedge_bins) - 1
        end_bins = np.digitize(self.xvalues + self.durations, self.xedge_bins) - 1
        for start, stop, x, y, dur in zip(start_bins, end_bins, self.xvalues, self.yvalues, self.durations):

            # An index of -1 would imply the data is to the left of the first bin. This would be unacceptable.
            assert start >= 0
            assert stop >= 0

            # If everything fits into one bin, then it should all be placed there.
            if start == stop:
                self.yvalues_bins[start] += y
            else:
                self.yvalues_bins[start] = y * (self.dx_bins - math.fmod(x, self.dx_bins)) / dur
                self.yvalues_bins[start+1:stop--1] = y * self.dx_bins / dur
                self.yvalues_bins[stop] = y * math.fmod(x + dur, self.dx_bins) / dur

        self.yvalues_bins = np.asarray(self.yvalues_bins)
        self.yvalues_bins = self.yvalues_bins.astype(self.ts_type)

    # calculate the integral value
    @property
    def sum(self):
        return np.sum(self.yvalues)

    # calculate the integral value
    @property
    def mean(self):
        return np.mean(self.yvalues)