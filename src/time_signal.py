import numpy as np
import re


# The availably types of time-series (or summed/averaged totals) data that we can use
signal_types = {

    # CPU
    'flops':         {'type': float, 'category': 'cpu',        'behaviour': 'sum'},

    # (file) I/O
    'kb_read':       {'type': float, 'category': 'file-read',  'behaviour': 'sum'},
    'kb_write':      {'type': float, 'category': 'file-write', 'behaviour': 'sum'},

    # MPI activity
    'n_pairwise':    {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'kb_pairwise':   {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'n_collective':  {'type': float, 'category': 'mpi',        'behaviour': 'sum'},
    'kb_collective': {'type': float, 'category': 'mpi',        'behaviour': 'sum'}
}


from tools import mytools


class TimeSignal(object):

    def __init__(self):

        self.name = None
        self.base_signal_name = None

        # vals in the time plane
        self.xvalues = None
        self.yvalues = None

        # vals in freq domain
        self.freqs = None
        self.ampls = None
        self.phases = None

        # digitized vals
        self.xvalues_bins = None
        self.yvalues_bins = None
        self.xedge_bins = None
        self.dx_bins = None

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

    # create from freq domain
    def create_ts_from_spectrum(self, name, time, freqs, ampls, phases, base_signal_name=None):

        self.name = name
        self.base_signal_name = base_signal_name if base_signal_name else self.name
        assert self.base_signal_name in signal_types

        self.xvalues = time
        self.yvalues = abs(mytools.freq_to_time(time - time[0], freqs, ampls, phases))

        self.freqs = freqs
        self.ampls = ampls
        self.phases = phases

    # create from time plane
    def create_ts_from_values(self, name, xvals, yvals, base_signal_name=None):

        self.name = name
        self.base_signal_name = base_signal_name if base_signal_name else self.name
        assert self.base_signal_name in signal_types

        self.xvalues = np.asarray(xvals)
        self.yvalues = np.asarray(yvals)
        self.yvalues = self.yvalues.astype(self.ts_type)

    # calculate bins
    def digitize(self, n_bins, key=None):

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

        self.yvalues_bins = np.zeros(len(self.xvalues_bins))+digitize_eps
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

    # calculate the integral value
    @property
    def sum(self):
        return np.sum(self.yvalues)

    # calculate the integral value
    @property
    def mean(self):
        return np.mean(self.yvalues)