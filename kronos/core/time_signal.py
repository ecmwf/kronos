# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections import OrderedDict

import numpy as np
import math
from kronos_tools import utils

# The availably types of time-series (or summed/averaged totals) data that we can use
signal_types = OrderedDict([

    # # CPU
    ('flops',    {'type': int,   'category': 'cpu',        'behaviour': 'sum', 'max_value': 1.0e20}),

    # (file) I/O
    ('kb_read',  {'type': float, 'category': 'file-read',  'behaviour': 'sum', 'max_value': 1.0e20}),
    ('kb_write', {'type': float, 'category': 'file-write', 'behaviour': 'sum', 'max_value': 1.0e20}),
    ('n_read',   {'type': int,   'category': 'file-read',  'behaviour': 'sum', 'max_value': 1000.0}),
    ('n_write',  {'type': int,   'category': 'file-write', 'behaviour': 'sum', 'max_value': 100.00}),

    # MPI activity
    ('n_pairwise',    {'type': int,   'category': 'mpi',  'behaviour': 'sum', 'max_value': 1.0e20}),
    ('kb_pairwise',   {'type': float, 'category': 'mpi',  'behaviour': 'sum', 'max_value': 1.0e20}),
    ('n_collective',  {'type': int,   'category': 'mpi',  'behaviour': 'sum', 'max_value': 1.0e20}),
    ('kb_collective', {'type': float, 'category': 'mpi',  'behaviour': 'sum', 'max_value': 1.0e20})
])


# add print info
float_format_print = '%14.1f'
int_format_print = '%14.0f'

signal_types['flops']["print_info"] = {"unit": "Gflops", "format": int_format_print, "conv": 1.0e-9}

signal_types['kb_read']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0e-6}
signal_types['kb_write']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0e-6}
signal_types['n_read']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['n_write']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}

signal_types['n_pairwise']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['kb_pairwise']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0e-6}
signal_types['n_collective']["print_info"] = {"unit": "times ", "format": int_format_print, "conv": 1.0}
signal_types['kb_collective']["print_info"] = {"unit": "Gbytes", "format": float_format_print, "conv": 1.0e-6}


time_signal_names = signal_types.keys()


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

        # # digitized vals
        # self.xvalues_bins = None
        # self.yvalues_bins = None
        # self.xedge_bins = None
        # self.dx_bins = None

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

        # sort xvals and yvals before passing them to the time-signal
        x_vec = np.asarray(xvals)
        x_vec = x_vec.reshape(x_vec.flatten().size, 1)

        y_vec = np.asarray(yvals)
        y_vec = y_vec.reshape(y_vec.flatten().size, 1)

        if x_vec.size != y_vec.size:
            raise ValueError("timesignal error: xvec size {} differ from yvec size {}". format(x_vec.shape, y_vec.shape))

        xy_vec = np.hstack((x_vec, y_vec))
        xy_vec_sort = xy_vec[xy_vec[:, 0].argsort()]

        return TimeSignal(
            name,
            base_signal_name=base_signal_name,
            durations=np.asarray(durations) if durations is not None else None,
            xvalues=xy_vec_sort[:, 0],
            yvalues=xy_vec_sort[:, 1],
            priority=priority
        )

    def digitized(self, nbins=None):
        """
        On-the-fly return digitized time series (rather than using
        """
        if nbins is None:
            return self.xvalues, self.yvalues

        # Digitization key
        key = self.digitization_key

        if self.durations is not None:
            return self.digitize_durations(nbins, key)

        # Determine the bin boundaries
        xedge_bins = np.linspace(min(self.xvalues), max(self.xvalues) + 1.0e-6, nbins + 1)

        # Return xvalues as the midpoints of the bins
        bins_delta = xedge_bins[1] - xedge_bins[0]
        xvalues = xedge_bins[1:] - (0.5 * bins_delta)

        # Split the data up amongst the bins
        # n.b. Returned indices will be >= 1, as 0 means to the left of the left-most edge.
        bin_indices = np.digitize(self.xvalues, xedge_bins)

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

    def digitize_durations(self, nbins, key=None):

        assert self.durations is not None

        # Use a sensible default
        if key is None:
            key = self.digitization_key

        # Find out what the maximum time is
        xedge_bins = np.linspace(0.0, max(self.xvalues + self.durations) + 1.0e-6, nbins + 1)
        dx_bins = xedge_bins[1] - xedge_bins[0]
        xvalues_bins = 0.5 * (xedge_bins[1:] + xedge_bins[:-1])
        yvalues_bins = np.zeros(len(xvalues_bins))

        # Now we need to loop through to attribute the data to bins (as considering the durations, it may not be
        # one-to-one
        start_bins = np.digitize(self.xvalues, xedge_bins) - 1
        end_bins = np.digitize(self.xvalues + self.durations, xedge_bins) - 1
        for start, stop, x, y, dur in zip(start_bins, end_bins, self.xvalues, self.yvalues, self.durations):

            # An index of -1 would imply the data is to the left of the first bin. This would be unacceptable.
            assert start >= 0
            assert stop >= 0

            # If everything fits into one bin, then it should all be placed there.
            if start == stop:
                yvalues_bins[start] += y
            else:
                yvalues_bins[start] = y * (dx_bins - math.fmod(x, dx_bins)) / dur
                yvalues_bins[start+1:stop--1] = y * dx_bins / dur
                yvalues_bins[stop] = y * math.fmod(x + dur, dx_bins) / dur

        yvalues_bins = np.asarray(yvalues_bins)
        yvalues_bins = yvalues_bins.astype(self.ts_type)

        return xvalues_bins, yvalues_bins

    # calculate the integral value
    @property
    def sum(self):
        return np.sum(self.yvalues)

    # calculate the integral value
    @property
    def mean(self):
        return np.mean(self.yvalues)