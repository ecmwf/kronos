#import numpy as np
#from pylab import *

#import fileinput
#import csv
#import re
#import os
#import subprocess

#import random
from logreader import LogReader


class PBSLogReader(LogReader):

    #===================================================================
    def __init__(self, Options):

        LogReader.__init__(self, Options)

        #self.LogData      = []
        #self.LogList      = []
        #self.out_dir      = Options.out_dir
        #self.darshan_dir  = Options.darshan_dir
        #self.fname_csv_out = Options.fname_csv_out

    #===================================================================
    def read_logs(self, filename_in):

        #---- init counters ---
        pbs_data = {}
        job_cc = 0
        self.LogList_raw = ['ctime',
                            'qtime',
                            'etime',
                            'end',
                            'start',
                            'resources_used.ncpus',
                            'resources_used.mem',
                            'resources_used.cpupercent',
                            'group',
                            'jobname',
                            ]

        self.LogList = ['time_created',
                        'time_queued',
                        'time_eligible',
                        'time_end',
                        'time_start',
                        'ncpus',
                        'memory_kb',
                        'cpu_percent',
                        'group',
                        'jobname',
                        ]
        #----------------------

        self.LogData = []

        #------------ Read file --------------
        f_in = open(filename_in, "r")

        for line in f_in:

            #----array got by splitting the line..
            larray = line.split(" ")
            b1_array = larray[1].split(";")

            #------------- block E -----------------
            if (b1_array[1] == "E"):

                #-- init dictionary ----
                line_dict = {}

                #----- user name ------
                user_name = b1_array[3].split("=")[1]
                line_dict["user"] = str(user_name)

                for jobL in range(0, len(larray)):

                    yval_val = larray[jobL].split("=")

                    if (len(yval_val) == 2):
                        if yval_val[0] in self.LogList_raw:

                            #-------- find index --------
                            idx = self.LogList_raw.index(yval_val[0])
                            key_name = self.LogList[idx]
                            line_dict[key_name] = yval_val[1].strip()

                #--------- derived quantities.. -----------
                line_dict["runtime"] = float(
                    line_dict["time_end"]) - float(line_dict["time_start"])
                #------------------------------------------

                #---------- sanitize datatypes ------------
                line_dict['time_created'] = int(line_dict['time_created'])
                line_dict['time_queued'] = int(line_dict['time_queued'])
                line_dict['time_eligible'] = int(line_dict['time_eligible'])
                line_dict['time_end'] = int(line_dict['time_end'])
                line_dict['time_start'] = int(line_dict['time_start'])
                line_dict['ncpus'] = int(line_dict['ncpus'])
                line_dict['memory_kb'] = int(line_dict['memory_kb'][:-2])
                line_dict['cpu_percent'] = float(line_dict['cpu_percent'])
                line_dict['group'] = str(line_dict['group'])
                line_dict['jobname'] = str(line_dict['jobname'])
                #------------------------------------------

                self.LogData.append(line_dict)
                job_cc = job_cc + 1
