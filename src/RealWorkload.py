import numpy as np
from pylab import *
from matplotlib import dates

import csv
import os
import datetime

from tools import *
from PlotHandler import PlotHandler
from RealJob import RealJob


class RealWorkload(object):

    """ Class main functionalities
        1) Contains a raw workload data
        2) Calculate derived data (e.g. app runtime, etc..)
        3) Display relevant statistics
    """

    #================================================================
    def __init__(self, ConfigOptions):

        self.LogData = []
        self.LogList = []
        self.total_metrics = {}

        self.input_dir = ConfigOptions.DIR_INPUT
        self.out_dir = ConfigOptions.DIR_OUTPUT
        self.total_metrics_nbins = ConfigOptions.REALWORKLOAD_TOTAL_METRICS_NBINS

        self.Njobs = ConfigOptions.DUMMY_WORKLOAD_NJOBS
        self.Nprocs_total = ConfigOptions.DUMMY_WORKLOAD_NPROCS_TOTAL
        self.time_schedule_total = ConfigOptions.DUMMY_WORKLOAD_TIME_SCHEDULE_TOTAL
        self.time_application_max = ConfigOptions.DUMMY_WORKLOAD_TIME_APPLICATION_MAX

        self.dir_input = ConfigOptions.DIR_INPUT
        self.job_output_all = np.array([]).reshape(3, 0)
        self.job_list = []

    #================================================================
    def read_PBS_logs(self, filename_in):

        #---- init counters ---
        pbs_data = {}
        self.LogList_raw = ['ctime',
                            'qtime',
                            'etime',
                            'end',
                            'start',
                            'resources_used.ncpus',
                            'Resource_List.ncpus',
                            'resources_used.mem',
                            'resources_used.cpupercent',
                            'resources_used.cput',
                            'group',
                            'jobname',
                            ]

        # - standardize the key nemes for different PBS log labels..
        self.LogList = ['time_created',
                        'time_queued',
                        'time_eligible',
                        'time_end',
                        'time_start',
                        'ncpus',
                        'ncpus',
                        'memory_kb',
                        'cpu_percent',
                        'cpu_percent',
                        'group',
                        'jobname',
                        ]
        #----------------------

        self.LogData = []

        #------------ Read file --------------
        f_in = open(filename_in, "r")
        cc = 1
        cce = 0
        for line in f_in:

            #----array got by splitting the line..
            larray = line.split(" ")
            b1_array = larray[1].split(";")

            # print "==========> ", cc

            #------------- block E -----------------
            if (b1_array[1] == "E"):

                #-- init dictionary ----
                line_dict = {}

                #----- user name ------
                user_name = b1_array[3].split("=")[1]
                line_dict["user"] = str(user_name)

                for jobL in range(0, len(larray)):

                    yval_val = larray[jobL].split("=")
                    # print yval_val

                    if (len(yval_val) == 2):
                        if yval_val[0] in self.LogList_raw:

                            #-------- find index --------
                            idx = self.LogList_raw.index(yval_val[0])
                            key_name = self.LogList[idx]
                            line_dict[key_name] = yval_val[1].strip()

                    #------- special case for ARCTUR PBS..
                    # Resource_List.nodes=1:ppn=1
                    if (yval_val[0] == "Resource_List.nodes"):
                        if len(yval_val[1].split(":")) > 1:
                            if yval_val[1].split(":")[1] == "ppn":
                                line_dict["ncpus"] = int(
                                    yval_val[2]) * int(yval_val[1].split(":")[0])
                        else:
                            line_dict["ncpus"] = int(yval_val[1])

                # print line_dict

                aJob = RealJob()

                # print 'aJob.time_created ', line_dict['time_created']
                # print 'aJob.time_queued  ', line_dict['time_queued']
                # print 'aJob.time_eligible', line_dict['time_eligible']
                # print 'aJob.time_end     ', line_dict['time_end']
                # print 'aJob.time_start   ', line_dict['time_start']
                # print int(line_dict['ncpus'])
                # print line_dict['time_created']
                # print type( line_dict['time_created'] )
                # print any(c.isalpha() for c in line_dict['time_created'])

                #from IPython.core.debugger import Tracer
                # Tracer()()

                #---- created  time
                if any([c.isalpha() for c in line_dict['time_created']]):
                    aJob.time_created = -1
                else:
                    aJob.time_created = int(line_dict['time_created'])

                #---- queue time
                if any([c.isalpha() for c in line_dict['time_queued']]):
                    aJob.time_queued = -1
                else:
                    aJob.time_queued = int(line_dict['time_queued'])

                #---- eligible time
                if any([c.isalpha() for c in line_dict['time_eligible']]):
                    aJob.time_eligible = -1
                else:
                    aJob.time_eligible = int(line_dict['time_eligible'])

                #---- end time
                if any([c.isalpha() for c in line_dict['time_end']]):
                    aJob.time_end = -1
                else:
                    aJob.time_end = int(line_dict['time_end'])

                #---- start time
                if any([c.isalpha() for c in line_dict['time_start']]):
                    aJob.time_start = -1
                else:
                    aJob.time_start = int(line_dict['time_start'])

                #---- average memory
                if any([c.isalpha() for c in line_dict['memory_kb'][:-2]]):
                    aJob.memory_kb = -1
                else:
                    aJob.memory_kb = int(line_dict['memory_kb'][:-2])

                if 'ncpus' in line_dict:
                    aJob.ncpus = int(line_dict['ncpus'])
                else:
                    aJob.ncpus = -1

                aJob.cpu_percent = float(
                    line_dict['cpu_percent'].replace(":", ""))
                aJob.group = str(line_dict['group'])
                aJob.jobname = str(line_dict['jobname'])
                aJob.user = str(line_dict['user'])

                # print 'aJob.time_created ', aJob.time_created
                # print 'aJob.time_queued  ', aJob.time_queued
                # print 'aJob.time_eligible', aJob.time_eligible
                # print 'aJob.time_end     ', aJob.time_end
                # print 'aJob.time_start   ', aJob.time_start
                # print 'aJob.memory_kb    ', aJob.memory_kb
                # print 'aJob.ncpus        ', aJob.ncpus
                # print 'aJob.cpu_percent  ', aJob.cpu_percent
                # print 'aJob.group        ', aJob.group
                # print 'aJob.jobname      ', aJob.jobname

                # raw_input("")

                self.LogData.append(aJob)
                cce += 1

                # if aJob.ncpus==240:
                # print cc
                # print aJob.ncpus
                # print aJob.jobname
                # print aJob.time_start

            cc += 1

    #================================================================
    #---------- derived metrics -------------
    def calculate_derived_quantities(self):

        #----- remove invalid entries ---------
        self.LogData[:] = [
            iJob for iJob in self.LogData if iJob.time_start != -1]
        self.LogData[:] = [
            iJob for iJob in self.LogData if iJob.time_end != -1]
        self.LogData[:] = [
            iJob for iJob in self.LogData if iJob.time_end >= iJob.time_start]
        self.LogData[:] = [
            iJob for iJob in self.LogData if iJob.time_queued != -1]
        self.LogData[:] = [
            iJob for iJob in self.LogData if iJob.time_start >= iJob.time_queued]
        self.LogData[:] = [iJob for iJob in self.LogData if iJob.ncpus > 0]

        self.LogData.sort(key=lambda x: x.time_start, reverse=False)

        self.minStartTime = min([x.time_start for x in self.LogData])
        self.maxStartTime = max([x.time_start for x in self.LogData])

        for iJob in self.LogData:
            iJob.runtime = float(iJob.time_end) - float(iJob.time_start)
            iJob.time_start_0 = iJob.time_start - self.minStartTime
            iJob.time_in_queue = iJob.time_start - iJob.time_queued

        self.maxStartTime_fromT0 = max(
            [iJob.time_start_0 for x in self.LogData])

#        from IPython.core.debugger import Tracer
#        Tracer()()

    #================================================================
    # def write_csv(self):

        # field_names = ['jobname',
        #'user',
        #'group',
        #'time_start',
        #'time_end',
        #'time_created',
        #'time_queued',
        #'time_eligible',
        #'time_start_0',
        #'time_mid_0',
        #'runtime',
        #'ncpus',
        #'memory_kb',
        #'cpu_percent',
        #'IO_N_read',
        #'IO_Kb_read',
        #'IO_N_write',
        #'IO_Kb_write',
        #]

        #f = open( self.out_dir + "/" + self.fname_csv_out, 'w')
        #w = csv.DictWriter(f, field_names )
        # w.writeheader()
        #w.writerows( self.LogData )
        # f.close()

    #================================================================
    def make_plots(self, Tag, date_ticks="month"):

        #------------ ncpu(t) -------------------
        iFig = PlotHandler.get_fig_handle_ID()
        Fhdl = figure(iFig)
        xname = "time_start_0"
        yname = "ncpus"
        vecXkeys = np.asarray([x.time_start for x in self.LogData])

        # convert epoch to matplotlib float format
        dts = map(datetime.datetime.fromtimestamp, vecXkeys)
        fds = dates.date2num(dts)  # converted

        vecYkeys = np.asarray([y.ncpus for y in self.LogData])
        title('x=' + str(xname) + ',  y=' + str(yname))

        plot(fds, vecYkeys, 'k')
        subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.3)
        ax = gca()

        if date_ticks == "month":
            ax.xaxis.set_major_locator(dates.MonthLocator())
            hfmt = dates.DateFormatter('%Y-%m/%d')
        elif date_ticks == "hour":
            ax.xaxis.set_major_locator(dates.HourLocator())
            hfmt = dates.DateFormatter('%m/%d %H:%M')

        ax.xaxis.set_major_formatter(hfmt)
        xticks(rotation='vertical')
        xticks(rotation=90)

        xlabel('date/time')
        ylabel(yname)
        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_data' +
                '_x=' + xname + '_y=' + yname + '.png')
        close(iFig)
        #----------------------------------------

        #------------ PDF(ncpus) ----------------
        iFig = PlotHandler.get_fig_handle_ID()
        yname = "ncpus"
        vecYkeys = np.asarray([y.ncpus for y in self.LogData])
        nbins = 40
        #hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=False)
        figure(iFig)
        title("Histogram of ncpus")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='b')
        yscale('log')
        xlabel('N CPU''s')
        savefig(self.out_dir + '/' + Tag + '_plot_' +
                'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)
        #----------------------------------------

        # ------------ PDF(memory Kb) ------------
        #iFig = PlotHandler.get_fig_handle_ID()
        #yname  = "memory_kb"
        #vecYkeys = np.asarray( [ y.memory_kb  for y in self.LogData ] )
        #nbins = 40
        #hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        #figure( iFig )
        #title("PDF of memory_kb")
        #xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        #bar( bin_edges[:-1] , hh, diff(bin_edges), color='r')
        # yscale('log')
        #xlabel( 'Average job memory [Kb]' )
        # savefig(self.out_dir+'/'+Tag+'_plot_'+'raw_data'+'_y='+yname+'_hist.png')
        # close(iFig)
        # ---------------------------------------

        #------------ PDF(runtime) -------------
        iFig = PlotHandler.get_fig_handle_ID()
        yname = "runtime"
        vecYkeys = np.asarray([y.runtime / 3600. for y in self.LogData])
        nbins = 40
        #hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=False)
        figure(iFig)
        title("Histogram of runtime")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='r')
        yscale('log')
        xlabel('Job runtime [Hr]')
        savefig(self.out_dir + '/' + Tag + '_plot_' +
                'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)
        #---------------------------------------

        #------------ PDF(queuing time) -------------
        iFig = PlotHandler.get_fig_handle_ID()
        yname = "Queuing time"
        vecYkeys = np.asarray([y.time_in_queue / 3600. for y in self.LogData])
        nbins = 40
        #hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=False)
        figure(iFig)
        title("Histogram of queuing time")
        xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        bar(bin_edges[:-1], hh, diff(bin_edges), color='g')
        yscale('log')
        xlabel('Job queuing time [Hr]')
        savefig(self.out_dir + '/' + Tag + '_plot_' +
                'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)
        #---------------------------------------

    #================================================================
    def create_dummy_wl(self):
      
        """#TODO: started re-importing this functionality but not yet completed! """

        #------ create list of jobs -----
        job_list = []

        for ijob in np.arange(0, self.Njobs):

            ncpus = max([int(rdm.random() * self.Nprocs_total), 1])
            runtime = max([rdm.random() * self.time_application_max / 2., 1.0])
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
        os.environ["LD_PRELOAD"] = self.dir_darshan_lib + "/" + "libdarshan.so"

        #---- clean the log folder -----
        os.system("rm -f " + self.dir_log_output + "/*darshan.gz")


#      #========== schedule 0: run in parallel .. ==============
#      os.system("rm -f " + self.dir_input + "/*.finished")
#      time0 = time.time()
#      t_since_start = 0
#
#      avail_nodes = self.Nprocs_total
#      while (t_since_start <= self.time_schedule_total+1 ):
#
#        #------ checks for starting jobs.. ------
#        for ijob in job_list:
#            if (not ijob['has_started']) and (t_since_start >= ijob['start_time']) and (avail_nodes > ijob['ncpus']):
#                run_str = "mpirun -np " + str(ijob['ncpus']) + " " + ijob['job_name'] + " > " + ijob['job_name']+".finished" + " &"
#                print run_str
#                os.system( run_str )
#                ijob['has_started'] = 1
#                avail_nodes = max( [avail_nodes - ijob['ncpus'],0])
#                print "started job " + ijob['jobID'] + " at t=: " + str(t_since_start) + " avail cpu: " ,avail_nodes
#        #----------------------------------------
#
#        #------ checks for finished jobs.. ------
#        for ijob in job_list:
#            if isfilenotempty(ijob['job_name']+".finished") and (not ijob['has_finished']):
#                ijob['has_finished'] = 1
#                avail_nodes = avail_nodes + ijob['ncpus']
#        #----------------------------------------

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
