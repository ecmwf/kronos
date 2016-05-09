import numpy as np
from pylab import *

import fileinput
import os

from TimeSignal import TimeSignal


class RealJob(object):

    def __init__(self):

        #-- from logs
        self.time_created = None
        self.time_queued = None
        self.time_eligible = None
        self.time_end = None
        self.time_start = None
        self.ncpus = None
        self.memory_kb = None
        self.cpu_percent = None
        self.group = None
        self.jobname = None

        #----- derived ---------
        self.runtime = None
        self.time_start_0 = None
        self.time_in_queue = None

        #------ added ----------
        self.time_from_t0_vec = None
        self.timesignals = []
        self.timesignals_clust = []
        self.job_impact_index_rel = None

    #--------- aggregate time signals.. ----------
    def append_time_signal(self, timesignal_in):
        self.timesignals.append(timesignal_in)

    #--------- aggregate CLUSTER time signals.. ----------
    def append_time_signal_clust(self, timesignal_in):
        self.timesignals_clust.append(timesignal_in)

    #----------- job impact index.. ------------
    def job_impact_index(self):

        if not self.timesignals:
            raise UserWarning(
                'no timesignal found! => impact_idx is set to zero')
        else:
            impact_idx = 0
            for iTsign in self.timesignals:
                impact_idx += np.trapz(abs(iTsign.yvalues), iTsign.xvalues)

        return impact_idx
