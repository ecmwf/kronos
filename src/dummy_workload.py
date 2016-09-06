import time
import random as rdm
import numpy as np
import os

from tools.mytools import isfilenotempty, freq_to_time

#///////////////////////////////////////////////////////////////


class DummyWorkload():

    #===================================================================
    def __init__(self, Options):

        self.Njobs = Options.dummy_workload_Njobs
        self.Nprocs_total = Options.dummy_workload_Nprocs_total
        self.time_schedule_total = Options.dummy_workload_time_schedule_total
        self.time_application_max = Options.dummy_workload_time_application_max

        self.dir_input = Options.dir_input
        self.dir_output = Options.dir_output

        self.dir_log_output = Options.dir_log_output
        self.dir_darshan_lib = Options.dir_darshan_lib
        self.dir_output = Options.dir_output
        self.dir_openmpi_src = Options.dir_openmpi_src
        self.dir_openmpi_lib = Options.dir_openmpi_lib
        self.dir_boost_src = Options.dir_boost_src
        self.dir_boost_lib = Options.dir_boost_lib

        self.name_dummy_master = Options.name_dummy_master
        #self.job_output_all = np.zeros((3,1))
        self.job_output_all = np.array([]).reshape(3, 0)

        self.job_list = []

    #===================================================================
    def create_workload(self, user_key):

        if (user_key == "darshan"):

            #------ create list of jobs -----
            job_list = []

            for ijob in np.arange(0, self.Njobs):

                ncpus = max([int(rdm.random() * self.Nprocs_total), 1])
                runtime = max(
                    [rdm.random() * self.time_application_max / 2., 1.0])
                start_time = int(rdm.random() * self.time_schedule_total)
                IO_N_read = int(rdm.random() * 10)
                IO_Kb_read = int(ncpus * (100 + rdm.random() * 5))
                IO_N_write = int(rdm.random() * 10)
                IO_Kb_write = int(ncpus * (100 + rdm.random() * 5))

                job_list.append({
                                'jobID': str(ijob),
                                'ncpus': ncpus,
                                'runtime': runtime,
                                'start_time': start_time,
                                'IO_N_read': IO_N_read,
                                'IO_Kb_read': IO_Kb_read,
                                'IO_N_write': IO_N_write,
                                'IO_Kb_write': IO_Kb_write,
                                'has_started': 0,
                                'has_finished': 0,
                                'job_name': self.dir_input + "/" + "JOB-ID" + str(ijob) + "_exe"
                                })
            #---------------------------------

            #-------- generate apps ----------
            for ijob in job_list:

                build_str = (
                    "mpic++"
                    " -I" + self.dir_boost_src + " -L" + self.dir_openmpi_lib + ""
                    " -I" + self.dir_openmpi_src + " -L" + self.dir_openmpi_lib + ""
                    " " + self.dir_input + "/" + self.name_dummy_master + " "
                    " -DTMAX=" + str(ijob['runtime']) +
                    " -DKB_READ=" + str(ijob['IO_Kb_read']) +
                    " -DNREAD=" + str(ijob['IO_N_read']) +
                    " -DFILE_READ_NAME=\\\"" + self.dir_input + "/read_input.in\\\"" +
                    " -DKB_WRITE=" + str(ijob['IO_Kb_write']) +
                    " -DNWRITE=" + str(ijob['IO_N_write']) +
                    " -DFILE_WRITE_NAME=\\\"" + self.dir_output + "/out-id=" + ijob['jobID'] + ".out\\\"" +
                    " -o " + ijob['job_name']
                )

                print build_str

                ijob['build_str'] = build_str

                os.system(build_str)
            #---------------------------------

            #---- run schedule (+darshan) ----
            os.system("module load darshan")
            os.environ["DARSHAN_LOG_DIR"] = self.dir_log_output
            os.environ["LD_PRELOAD"] = self.dir_darshan_lib + \
                "/" + "libdarshan.so"

            #---- clean the log folder -----
            os.system("rm -f " + self.dir_log_output + "/*darshan.gz")

            #========== schedule 0: run in parallel .. ==============
            os.system("rm -f " + self.dir_input + "/*.finished")
            time0 = time.time()
            t_since_start = 0

            avail_nodes = self.Nprocs_total
            while (t_since_start <= self.time_schedule_total + 1):

                #------ checks for starting jobs.. ------
                for ijob in job_list:
                    if (not ijob['has_started']) and (t_since_start >= ijob['start_time']) and (avail_nodes > ijob['ncpus']):
                        run_str = "mpirun -np " + \
                            str(ijob['ncpus']) + " " + ijob['job_name'] + \
                            " > " + ijob['job_name'] + ".finished" + " &"
                        print run_str
                        os.system(run_str)
                        ijob['has_started'] = 1
                        avail_nodes = max([avail_nodes - ijob['ncpus'], 0])
                        print "started job " + ijob['jobID'] + " at t=: " + str(t_since_start) + " avail cpu: ", avail_nodes
                #----------------------------------------

                #------ checks for finished jobs.. ------
                for ijob in job_list:
                    if isfilenotempty(ijob['job_name'] + ".finished") and (not ijob['has_finished']):
                        ijob['has_finished'] = 1
                        avail_nodes = avail_nodes + ijob['ncpus']
                #----------------------------------------

                t_since_start = time.time() - time0

        #------------- generate time signals of the application metrics -------
        if (user_key == "time_plane"):

            #------ create list of jobs -----
            job_list = []

            for ijob in np.arange(0, self.Njobs):

                ncpus = max([int(rdm.random() * self.Nprocs_total), 1])
                runtime = max(
                    [rdm.random() * self.time_application_max / 2., 1.0])
                start_time = int(rdm.random() * self.time_schedule_total)

                times = np.linspace(0, runtime, 10)

                freqsR = np.random.random((10,)) * 1. / runtime * 10.
                amplsR = np.random.random((10,)) * 1000
                phasesR = np.random.random((10,)) * 2 * np.pi

                freqsW = np.random.random((10,)) * 1. / runtime * 10.
                amplsW = np.random.random((10,)) * 1000
                phasesW = np.random.random((10,)) * 2 * np.pi

                IO_N_read_vec = freq_to_time(times, freqsR, amplsR, phasesR)
                IO_N_write_vec = freq_to_time(times, freqsW, amplsW, phasesW)

                # print "iJob="+str(ijob) + "  iJob="+str(freqsR) + "  iJob="+str(amplsR) + "  iJob="+str(phasesR)
                # print "iJob="+str(ijob) + "
                # IO_N_read_vec="+str(IO_N_read_vec)

                job_list.append({
                                'jobID': str(ijob),
                                'ncpus': ncpus,
                                'runtime': runtime,
                                'start_time': start_time,
                                'times': times,
                                'freqsR': freqsR,
                                'amplsR': amplsR,
                                'phasesR': phasesR,
                                'freqsW': freqsW,
                                'amplsW': amplsW,
                                'phasesW': phasesW,
                                'IO_N_read_vec': IO_N_read_vec,
                                'IO_N_write_vec': IO_N_write_vec,
                                'has_started': 0,
                                'has_started_at': 0,
                                'has_finished': 0,
                                'has_finished_at': 0,
                                'job_name': self.dir_input + "/" + "JOB-ID" + str(ijob) + "_exe"
                                })

                # print "job="+str(ijob)+" ncpus="+str(ncpus)+"
                # start_time="+str(start_time)

            #========== schedule 0: run in parallel .. ==============
            time0 = time.time()
            t_since_start = 0

            avail_nodes = self.Nprocs_total
            while (t_since_start <= self.time_schedule_total + 1):

                #------ checks for starting jobs.. ------
                for ijob in job_list:
                    if (not ijob['has_started']) and (t_since_start >= ijob['start_time']) and (avail_nodes > ijob['ncpus']):
                        ijob['has_started'] = 1
                        ijob['has_started_at'] = t_since_start
                        avail_nodes = max([avail_nodes - ijob['ncpus'], 0])
                        print "started job " + ijob['jobID'] + " at t=: " + str(t_since_start) + " avail cpu: ", avail_nodes
                #----------------------------------------

                #------ checks for finished jobs.. ------
                for ijob in job_list:
                    if ijob['has_started'] and (t_since_start >= (ijob['has_started_at'] + ijob['runtime'])) and (not ijob['has_finished']):
                        print "at t=: " + str(t_since_start) + "  job:" + ijob['jobID'] + " has finished" + " avail cpu: ", avail_nodes
                        ijob['has_finished'] = 1
                        ijob['has_finished_at'] = t_since_start
                        avail_nodes = avail_nodes + ijob['ncpus']

                        v1 = ijob['times'] + ijob['has_started_at']
                        v2 = ijob['IO_N_read_vec']
                        v3 = ijob['IO_N_write_vec']
                        vvv = np.vstack((v1, v2, v3))
                        self.job_output_all = np.hstack(
                            (self.job_output_all, vvv))
                #----------------------------------------

                t_since_start = time.time() - time0

            # print self.job_output_all
            # print shape( self.job_output_all )
            # print "----------------------"

            # print self.job_output_all[0,:].argsort()
            self.job_output_all = self.job_output_all[
                :, self.job_output_all[0, :].argsort()]
            # print self.job_output_all

            self.job_list = job_list

    #===================================================================


# ////////////////////////////////////////////////////////////////
#if __name__ == "__main__": create_dummy_workload()
# ////////////////////////////////////////////////////////////////
