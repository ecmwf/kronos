import numpy as np

from tools import *


class TimeSignal(object):

    def __init__(self):

        self.name = ''

        # vals in the time plane
        self.xvalues = None
        self.yvalues = None

        #-- vals in freq domain
        self.freqs = None
        self.ampls = None
        self.phases = None

        #-- digitized vals
        self.xvalues_bins = None
        self.yvalues_bins = None
        self.xedge_bins = None

    #------- create from freq domain
    def create_ts_from_spectrum(self, name, time, freqs, ampls, phases):

        self.name = name

        self.xvalues = time
        self.yvalues = abs(freq_to_time(time - time[0], freqs, ampls, phases))

        self.freqs = freqs
        self.ampls = ampls
        self.phases = phases

    #------- create from time plane
    def create_ts_from_values(self, name, xvals, yvals):

        self.name = name
        self.xvalues = xvals
        self.yvalues = yvals

    #---- calculate bins ----
    def digitize(self, Nbins, key):

        self.xedge_bins = np.linspace(
            min(self.xvalues), max(self.xvalues), Nbins + 1)
        self.DX_bins = self.xedge_bins[1] - self.xedge_bins[0]

        digitized_data = np.digitize(self.xvalues, self.xedge_bins)
        self.xvalues_bins = 0.5 * (self.xedge_bins[1:] + self.xedge_bins[:-1])

        self.yvalues_bins = []
        for i in range(1, len(self.xedge_bins)):
            mean_val = 1e-6
            if any(self.yvalues[digitized_data == i]):
                if key == 'mean':
                    mean_val = self.yvalues[digitized_data == i].mean()
                elif key == 'sum':
                    mean_val = self.yvalues[digitized_data == i].sum()
                else:
                    raise ValueError('option not recognised!')

            self.yvalues_bins.append(mean_val)
