import numpy as np
from pylab import *

import fileinput
import os


class ScheduledJob(object):
  
  "TODO: update this class: obsolete!"

    #===================================================================
    def __init__(self):

        self.jobID = []
        self.jobpath = []

        self.synth_app_exe = []
        self.time_start = []

        self.ncpu = []
        self.mem_req = []

        self.script_name = []
        self.script_logfile = []
        self.has_run = []

    #===================================================================
    def write_script(self):

        f = open(self.jobpath + self.script_name, 'w')
        f.write("just an example.. to be implemented..\n")

        #w = csv.DictWriter(f, field_names )
        # w.writeheader()
        #w.writerows( self.LogData )
        f.close()

    #===================================================================
    def run_job(self):

        self.script_logfile = self.synth_app_exe + ".log"
        os.system("mpirun -np " + str(self.ncpu) + " " +
                  self.synth_app_exe + " 1> " + self.script_logfile + " &")
