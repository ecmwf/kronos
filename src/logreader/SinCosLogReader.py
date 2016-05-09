import numpy as np
from pylab import *

import fileinput
import csv
import re
import os
import subprocess

import random
from logreader import LogReader
from tools import *


class SinCosLogReader(LogReader):

    #===================================================================
    def __init__(self, Options):
        LogReader.__init__(self, Options)
        #self.darshan_dir  = Options.darshan_dir
    #===================================================================

    #===================================================================
    def read_logs(self, workload):

        line_dict = {}
        job_cc = 0

        for ijob in workload.job_list:

            #------------- fill up fields ---------------
            line_dict["user"] = "dummy_user"
            line_dict['group'] = 'default-group'
            line_dict['jobname'] = ijob['job_name']
            line_dict['time_created'] = -1
            line_dict['time_queued'] = -1
            line_dict['time_eligible'] = -1
            line_dict['time_end'] = ijob['has_finished_at']
            line_dict['time_start'] = ijob['has_started_at']
            line_dict["runtime"] = ijob['runtime']
            line_dict['ncpus'] = ijob['ncpus']
            line_dict['memory_kb'] = -1
            line_dict['cpu_percent'] = -1
            line_dict['IO_N_read'] = -1
            line_dict['IO_Kb_read'] = -1
            line_dict['IO_N_write'] = -1
            line_dict['IO_Kb_write'] = -1

            line_dict['times'] = ijob['times']
            line_dict['IO_N_read_vec'] = ijob['IO_N_read_vec']
            line_dict['IO_N_write_vec'] = ijob['IO_N_write_vec']

            line_dict['freqsR'] = ijob['freqsR']
            line_dict['amplsR'] = ijob['amplsR']
            line_dict['phasesR'] = ijob['phasesR']
            line_dict['freqsW'] = ijob['freqsW']
            line_dict['amplsW'] = ijob['amplsW']
            line_dict['phasesW'] = ijob['phasesW']
            #--------------------------------------------

        self.LogData.append(line_dict)
        job_cc = job_cc + 1

        #------ data aggregated per job ID -------
        self.LogData = multikeysort(self.LogData, ['time_start'])

        minStartTime = min(self.LogData, key=lambda x: x['time_start'])
        #===================================================================
