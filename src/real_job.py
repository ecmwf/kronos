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
        self.nnodes = None
        self.memory_kb = None
        # self.cpu_percent = None
        self.group = None
        self.jobname = None
        self.user = None
        self.queue_type = None

        # derived
        self.runtime = None
        self.time_start_0 = None
        self.time_in_queue = None
        self.idx_in_log = None

        # added
        self.timesignals = []
        self.job_impact_index_rel = None
        self.__job_impact_index = None

    # aggregate time signals..
    def append_time_signal(self, time_signal_in):
        self.timesignals.append(time_signal_in)

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

    def check_job(self):

        # from logs
        if self.time_created is None: raise UserWarning("job: " + self.jobname + ", Qty: time_created")
        if self.time_queued is None: raise UserWarning("job: " + self.jobname + ", Qty: time_queued")
        if self.time_eligible is None: raise UserWarning("job: " + self.jobname + ", Qty: time_eligible")
        if self.time_end is None: raise UserWarning("job: " + self.jobname + ", Qty: time_end")
        if self.time_start is None: raise UserWarning("job:"  + self.jobname + ", Qty: time_start")
        if self.ncpus is None: raise UserWarning("job: " + self.jobname + ", Qty: ncpus")
        if self.nnodes is None: raise UserWarning("job: " + self.jobname + ", Qty: nnodes")
        if self.memory_kb is None: raise UserWarning("job: " + self.jobname + ", Qty: memory_kb")

        # self.cpu_percent is None
        if self.group is None: raise UserWarning("job: " + self.jobname + ", Qty: group")
        if self.jobname is None: raise UserWarning("job: " + self.jobname + ", Qty: jobname")
        if self.user is None: raise UserWarning("job: " + self.jobname + ", Qty: user")
        if self.queue_type is None: raise UserWarning("job: " + self.jobname + ", Qty: queue_type")


        # derived
        if self.runtime is None: raise UserWarning("job: " + self.jobname + ", Qty: runtime")
        if self.time_start_0 is None: raise UserWarning("job: " + self.jobname + ", Qty: time_start_0")
        if self.time_in_queue is None: raise UserWarning("job: " + self.jobname + ", Qty: time_in_queue")

        # added
        if self.timesignals is []: raise UserWarning("job: " + self.jobname + ", Qty: timesignals")
        if self.job_impact_index_rel is None: raise UserWarning("job: " + self.jobname + ", Qty: job_impact_index_rel")
        if self.__job_impact_index is None: raise UserWarning("job: " + self.jobname + ", Qty: __job_impact_index")
