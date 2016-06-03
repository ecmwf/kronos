import numpy as np


class RealJob(object):

    def __init__(self):

        # from logs
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
        self.user = None

        # derived
        self.runtime = None
        self.time_start_0 = None
        self.time_in_queue = None

        # added
        self.time_from_t0_vec = None
        self.timesignals = []
        self.job_impact_index_rel = None
        self.__job_impact_index = None

    # aggregate time signals..
    def append_time_signal(self, timesignal_in):
        self.timesignals.append(timesignal_in)

    # job impact index..
    @property
    def job_impact_index(self):

        if not self.timesignals:
            raise UserWarning('no timesignal found! => _impact_idx is set to zero')
        else:
            self.__job_impact_index = 0
            for i_ts in self.timesignals:
                self.__job_impact_index += np.trapz(abs(i_ts.yvalues), i_ts.xvalues)

        return self.__job_impact_index

    # @job_impact_index.setter
    # def job_impact_index(self, value):
    #     self.__job_impact_index = value
