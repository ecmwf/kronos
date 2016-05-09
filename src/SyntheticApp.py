import numpy as np
from pylab import *
import os


class SyntheticApp:
  
  "TODO: update this class: obsolete!"

    #===================================================================
    def __init__(self):

        self.jobID = []
        self.job_name = []

        self.runtime_max = []
        self.source_path = []
        self.name_exe = []

        self.cpu_percent = []
        self.mem_percent = []

        self.IO_N_read = []
        self.IO_Kb_read = []

        self.IO_N_write = []
        self.IO_Kb_write = []

    #===================================================================
    def create_executable(self):

        self.name_exe = "../output/JOB-ID" + str(self.jobID) + "_exe"

        build_str = (
            "mpic++"
            " -I/usr/local/apps/boost/1.55.0/include -L/usr/local/apps/boost/1.55.0/lib"
            " -I/usr/lib64/mpi/gcc/openmpi/include -L/usr/lib64/mpi/gcc/openmpi/lib64"
            " ../input/app_master.cpp "
            " -DTMAX=" + str(self.runtime_max) +
            " -DKB_READ=" + str(self.IO_Kb_read) +
            " -DNREAD=" + str(self.IO_N_read) +
            " -DFILE_READ_NAME=\\\"" + "../input/read_input.in\\\"" +
            " -DKB_WRITE=" + str(self.IO_Kb_write) +
            " -DNWRITE=" + str(self.IO_N_write) +
            " -DFILE_WRITE_NAME=\\\"" + "../output/out-id=" + str(self.jobID) + ".out\\\"" +
            " -o " + self.name_exe
        )

        print build_str
        os.system(build_str)
