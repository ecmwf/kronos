import numpy as np
from pylab import *
from matplotlib import dates

import os
import datetime

from tools import *
from plot_handler import PlotHandler
from real_job import RealJob
from time_signal import TimeSignal

class RealWorkload(object):

    """ Class main functionalities
        1) Contains a raw workload data
        2) Calculate derived data (e.g. app runtime, etc..)
        3) Display relevant statistics
    """

    def __init__(self, config_options):

        self.LogData = []
        self.LogList = []

        self.input_dir = config_options.DIR_INPUT
        self.out_dir = config_options.DIR_OUTPUT
        self.total_metrics_nbins = config_options.REALWORKLOAD_TOTAL_METRICS_NBINS

        self.Njobs = config_options.DUMMY_WORKLOAD_NJOBS
        self.Nprocs_total = config_options.DUMMY_WORKLOAD_NPROCS_TOTAL
        self.time_schedule_total = config_options.DUMMY_WORKLOAD_TIME_SCHEDULE_TOTAL
        self.time_application_max = config_options.DUMMY_WORKLOAD_TIME_APPLICATION_MAX

        self.dir_input = config_options.DIR_INPUT
        self.job_output_all = np.array([]).reshape(3, 0)
        self.job_list = []

        self.LogList_raw = []

        # total metrics and parameters
        self.total_metrics = []
        self.minStartTime = None
        self.maxStartTime = None
        self.maxStartTime_fromT0 = None

        # parameters for the correction part..
        self.Ntime = config_options.WORKLOADCORRECTOR_NTIME
        self.Nfreq = config_options.WORKLOADCORRECTOR_NFREQ
        self.Jobs_Nbins = config_options.WORKLOADCORRECTOR_JOBS_NBINS
        self.job_signal_names = [i_ts[0] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_type  = [i_ts[1] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_group = [i_ts[2] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]

    def read_PBS_logs(self, filename_in):

        # init counters
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

        self.LogData = []

        # Read file
        f_in = open(filename_in, "r")
        cc = 1
        cce = 0
        for line in f_in:

            # array got by splitting the line
            larray = line.split(" ")
            b1_array = larray[1].split(";")

            # block E
            if (b1_array[1] == "E"):

                # init dictionary
                line_dict = {}

                # user name
                user_name = b1_array[3].split("=")[1]
                line_dict["user"] = str(user_name)

                for jobL in range(0, len(larray)):

                    yval_val = larray[jobL].split("=")
                    # print yval_val

                    if (len(yval_val) == 2):
                        if yval_val[0] in self.LogList_raw:

                            # find index
                            idx = self.LogList_raw.index(yval_val[0])
                            key_name = self.LogList[idx]
                            line_dict[key_name] = yval_val[1].strip()

                    # special case for ARCTUR PBS..
                    # Resource_List.nodes=1:ppn=1
                    if yval_val[0] == "Resource_List.nodes":
                        if len(yval_val[1].split(":")) > 1:
                            if yval_val[1].split(":")[1] == "ppn":
                                line_dict["ncpus"] = int(
                                    yval_val[2]) * int(yval_val[1].split(":")[0])
                        else:
                            line_dict["ncpus"] = int(yval_val[1])

                # print line_dict

                i_job = RealJob()

                # print 'i_job.time_created ', line_dict['time_created']
                # print 'i_job.time_queued  ', line_dict['time_queued']
                # print 'i_job.time_eligible', line_dict['time_eligible']
                # print 'i_job.time_end     ', line_dict['time_end']
                # print 'i_job.time_start   ', line_dict['time_start']
                # print int(line_dict['ncpus'])
                # print line_dict['time_created']
                # print type( line_dict['time_created'] )
                # print any(c.isalpha() for c in line_dict['time_created'])

                # created  time
                if any([c.isalpha() for c in line_dict['time_created']]):
                    i_job.time_created = -1
                else:
                    i_job.time_created = int(line_dict['time_created'])

                # queue time
                if any([c.isalpha() for c in line_dict['time_queued']]):
                    i_job.time_queued = -1
                else:
                    i_job.time_queued = int(line_dict['time_queued'])

                # eligible time
                if any([c.isalpha() for c in line_dict['time_eligible']]):
                    i_job.time_eligible = -1
                else:
                    i_job.time_eligible = int(line_dict['time_eligible'])

                # end time
                if any([c.isalpha() for c in line_dict['time_end']]):
                    i_job.time_end = -1
                else:
                    i_job.time_end = int(line_dict['time_end'])

                # start time
                if any([c.isalpha() for c in line_dict['time_start']]):
                    i_job.time_start = -1
                else:
                    i_job.time_start = int(line_dict['time_start'])

                # average memory
                if any([c.isalpha() for c in line_dict['memory_kb'][:-2]]):
                    i_job.memory_kb = -1
                else:
                    i_job.memory_kb = int(line_dict['memory_kb'][:-2])

                if 'ncpus' in line_dict:
                    i_job.ncpus = int(line_dict['ncpus'])
                else:
                    i_job.ncpus = -1

                i_job.cpu_percent = float(line_dict['cpu_percent'].replace(":", ""))
                i_job.group = str(line_dict['group'])
                i_job.jobname = str(line_dict['jobname'])
                i_job.user = str(line_dict['user'])

                # print 'i_job.time_created ', i_job.time_created
                # print 'i_job.time_queued  ', i_job.time_queued
                # print 'i_job.time_eligible', i_job.time_eligible
                # print 'i_job.time_end     ', i_job.time_end
                # print 'i_job.time_start   ', i_job.time_start
                # print 'i_job.memory_kb    ', i_job.memory_kb
                # print 'i_job.ncpus        ', i_job.ncpus
                # print 'i_job.cpu_percent  ', i_job.cpu_percent
                # print 'i_job.group        ', i_job.group
                # print 'i_job.jobname      ', i_job.jobname

                # raw_input("")

                self.LogData.append(i_job)
                cce += 1

                # if i_job.ncpus==240:
                # print cc
                # print i_job.ncpus
                # print i_job.jobname
                # print i_job.time_start

            cc += 1

    # derived metrics
    def calculate_derived_quantities(self):

        # remove invalid entries
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.time_start != -1]
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.time_end != -1]
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.time_end >= i_job.time_start]
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.time_queued != -1]
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.time_start >= i_job.time_queued]
        self.LogData[:] = [i_job for i_job in self.LogData if i_job.ncpus > 0]
        self.LogData.sort(key=lambda x: x.time_start, reverse=False)

        self.minStartTime = min([i_job.time_start for i_job in self.LogData])
        self.maxStartTime = max([i_job.time_start for i_job in self.LogData])

        for i_job in self.LogData:
            i_job.runtime = float(i_job.time_end) - float(i_job.time_start)
            i_job.time_start_0 = i_job.time_start - self.minStartTime
            i_job.time_in_queue = i_job.time_start - i_job.time_queued

        # find latest job
        self.maxStartTime_fromT0 = max([i_job.time_start_0 for i_job in self.LogData])

    def enrich_data_with_TS(self, user_key):

        """ Enrich data with time series """

        if user_key == "FFT":

            for iJob in self.LogData:

                iJob.time_from_t0_vec = np.linspace(0, iJob.runtime, self.Ntime) + iJob.time_start_0

                for iTS in range(0, len(self.job_signal_names)):
                    freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
                    ampls = np.random.random((self.Nfreq,)) * 1000
                    phases = np.random.random((self.Nfreq,)) * 2 * np.pi

                    sig_name = self.job_signal_names[iTS]
                    sig_type = self.job_signal_type[iTS]
                    sig_group = self.job_signal_group[iTS]

                    TS = TimeSignal()
                    TS.create_ts_from_spectrum(sig_name, sig_type, sig_group, iJob.time_from_t0_vec, freqs, ampls,
                                               phases)
                    iJob.append_time_signal(TS)

            # NOTE: this assumes that all the jobs have the same number and
            # names of Time signals
            n_ts_in_job = len(self.LogData[0].timesignals)
            names_ts_in_job = [i_ts.name for i_ts in self.LogData[0].timesignals]

            # aggregates all the signals
            total_time = np.asarray([item for iJob in self.LogData for item in iJob.time_from_t0_vec])

            # loop over TS signals of each job
            for iTS in range(0, n_ts_in_job):
                name_ts = 'total_' + names_ts_in_job[iTS]
                vals = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[iTS].yvalues])
                TS = TimeSignal()
                TS.create_ts_from_values(name_ts, total_time, vals)
                self.total_metrics.append(TS)

        elif user_key == "bins":

            for iJob in self.LogData:

                iJob.time_from_t0_vec = np.linspace(0, iJob.runtime, self.Ntime) + iJob.time_start_0

                for iTS in range(0, len(self.job_signal_names)):
                    freqs = np.random.random((self.Nfreq,)) * 1. / iJob.runtime * 10.
                    ampls = np.random.random((self.Nfreq,)) * 1000
                    phases = np.random.random((self.Nfreq,)) * 2 * np.pi

                    sig_name = self.job_signal_names[iTS]
                    sig_type = self.job_signal_type[iTS]
                    sig_group = self.job_signal_group[iTS]

                    TS = TimeSignal()
                    TS.create_ts_from_spectrum(sig_name, sig_type, sig_group, iJob.time_from_t0_vec, freqs, ampls,
                                               phases)
                    TS.digitize(self.Jobs_Nbins, 'mean')
                    iJob.append_time_signal(TS)

            # NOTE: this assumes that all the jobs have the same number and names of Time signals
            n_ts_in_job = len(self.LogData[0].timesignals)
            names_ts_in_job = [i_ts.name for i_ts in self.LogData[0].timesignals]
            ts_types = [i_ts.ts_type for i_ts in self.LogData[0].timesignals]
            ts_groups = [i_ts.ts_group for i_ts in self.LogData[0].timesignals]

            # aggregates all the signals
            times_bins = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[0].xvalues_bins])
            for iTS in range(0, n_ts_in_job):
                name_ts = 'total_' + names_ts_in_job[iTS]
                yvals = np.asarray([item for iJob in self.LogData for item in iJob.timesignals[iTS].yvalues_bins])
                TS = TimeSignal()
                TS.create_ts_from_values(name_ts, ts_types[iTS], ts_groups[iTS], times_bins, yvals)
                self.total_metrics.append(TS)

        else:

            raise ValueError('option not recognised!')

    def calculate_global_metrics(self):

        """ Calculate all the relevant global metrics from the totals """

        for i_tot in self.total_metrics:
            i_tot.digitize(10, 'sum')

        # calculate relative impact factors (0 to 1)....
        imp_fac_all = [iJob.job_impact_index for iJob in self.LogData]
        for iJob in self.LogData:
            iJob.job_impact_index_rel = (iJob.job_impact_index - min(imp_fac_all)) / \
                                        (max(imp_fac_all) - min(imp_fac_all))

    def make_plots(self, Tag, date_ticks="month"):

        # ncpu(t)
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
        # elif date_ticks == "hour":
        else:
            ax.xaxis.set_major_locator(dates.HourLocator())
            hfmt = dates.DateFormatter('%m/%d %H:%M')

        ax.xaxis.set_major_formatter(hfmt)
        xticks(rotation='vertical')
        xticks(rotation=90)

        xlabel('date/time')
        ylabel(yname)
        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_data' + '_x=' + xname + '_y=' + yname + '.png')
        close(iFig)

        # --------- PDF(ncpus) -----------
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
        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)

        # ------------ PDF(memory Kb) ------------
        # iFig = PlotHandler.get_fig_handle_ID()
        # yname  = "memory_kb"
        # vecYkeys = np.asarray( [ y.memory_kb  for y in self.LogData ] )
        # nbins = 40
        # hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
        # figure( iFig )
        # title("PDF of memory_kb")
        # xx = (bin_edges[1:] + bin_edges[:-1]) / 2
        # bar( bin_edges[:-1] , hh, diff(bin_edges), color='r')
        # yscale('log')
        # xlabel( 'Average job memory [Kb]' )
        # savefig(self.out_dir+'/'+Tag+'_plot_'+'raw_data'+'_y='+yname+'_hist.png')
        # close(iFig)

        # ------------ PDF(runtime) -------------
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
        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)
        # ---------------------------------------

        # ------------ PDF(queuing time) -------------
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
        savefig(self.out_dir + '/' + Tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png')
        close(iFig)

#     def create_dummy_wl(self):
#
#         """#TODO: started re-importing this functionality but not yet completed! """
#
#         # ----- create list of jobs -----
#         job_list = []
#
#         for ijob in np.arange(0, self.Njobs):
#
#             ncpus = max([int(rdm.random() * self.Nprocs_total), 1])
#             runtime = max([rdm.random() * self.time_application_max / 2., 1.0])
#             start_time = int(rdm.random() * self.time_schedule_total)
#             IO_N_read = int(rdm.random() * 10)
#             IO_Kb_read = int(ncpus * (100 + rdm.random() * 5))
#             IO_N_write = int(rdm.random() * 10)
#             IO_Kb_write = int(ncpus * (100 + rdm.random() * 5))
#
#             job_list.append({
#                 'jobID': str(ijob),
#                 'ncpus': ncpus,
#                 'runtime': runtime,
#                 'start_time': start_time,
#                 'IO_N_read': IO_N_read,
#                 'IO_Kb_read': IO_Kb_read,
#                 'IO_N_write': IO_N_write,
#                 'IO_Kb_write': IO_Kb_write,
#                 'has_started': 0,
#                 'has_finished': 0,
#                 'job_name': self.dir_input + "/" + "JOB-ID" + str(ijob) + "_exe"
#             })
#
#         # -------- generate apps ----------
#         for ijob in job_list:
#
#             build_str = (
#                 "mpic++"
#                 " -I" + self.dir_boost_src + " -L" + self.dir_openmpi_lib + ""
#                 " -I" + self.dir_openmpi_src + " -L" + self.dir_openmpi_lib + ""
#                 " " + self.dir_input + "/" + self.name_dummy_master + " "
#                 " -DTMAX=" + str(ijob['runtime']) +
#                 " -DKB_READ=" + str(ijob['IO_Kb_read']) +
#                 " -DNREAD=" + str(ijob['IO_N_read']) +
#                 " -DFILE_READ_NAME=\\\"" + self.dir_input + "/read_input.in\\\"" +
#                 " -DKB_WRITE=" + str(ijob['IO_Kb_write']) +
#                 " -DNWRITE=" + str(ijob['IO_N_write']) +
#                 " -DFILE_WRITE_NAME=\\\"" + self.dir_output + "/out-id=" + ijob['jobID'] + ".out\\\"" +
#                 " -o " + ijob['job_name']
#             )
#
#             print build_str
#
#             ijob['build_str'] = build_str
#
#             os.system(build_str)
#         # ---------------------------------
#
#         # ---- run schedule (+darshan) ----
#         os.system("module load darshan")
#         os.environ["DARSHAN_LOG_DIR"] = self.dir_log_output
#         os.environ["LD_PRELOAD"] = self.dir_darshan_lib + "/" + "libdarshan.so"
#
#         # ---- clean the log folder -----
#         os.system("rm -f " + self.dir_log_output + "/*darshan.gz")
#
#
# #      # ========== schedule 0: run in parallel .. ==============
# #      os.system("rm -f " + self.dir_input + "/*.finished")
# #      time0 = time.time()
# #      t_since_start = 0
# #
# #      avail_nodes = self.Nprocs_total
# #      while (t_since_start <= self.time_schedule_total+1 ):
# #
# #        #------ checks for starting jobs.. ------
# #        for ijob in job_list:
# #            if (not ijob['has_started']) and (t_since_start >= ijob['start_time']) and (avail_nodes > ijob['ncpus']):
# #                run_str = "mpirun -np " + str(ijob['ncpus']) + " " + ijob['job_name'] + " > " + ijob['job_name']+".finished" + " &"
# #                print run_str
# #                os.system( run_str )
# #                ijob['has_started'] = 1
# #                avail_nodes = max( [avail_nodes - ijob['ncpus'],0])
# #                print "started job " + ijob['jobID'] + " at t=: " + str(t_since_start) + " avail cpu: " ,avail_nodes
# #        #----------------------------------------
# #
# #        #------ checks for finished jobs.. ------
# #        for ijob in job_list:
# #            if isfilenotempty(ijob['job_name']+".finished") and (not ijob['has_finished']):
# #                ijob['has_finished'] = 1
# #                avail_nodes = avail_nodes + ijob['ncpus']
# #        #----------------------------------------
#
#         # ========== schedule 0: run in parallel .. ==============
#         time0 = time.time()
#         t_since_start = 0
#
#         avail_nodes = self.Nprocs_total
#         while (t_since_start <= self.time_schedule_total + 1):
#
#             # ------ checks for starting jobs.. ------
#             for ijob in job_list:
#                 if (not ijob['has_started']) and (t_since_start >= ijob['start_time']) and (avail_nodes > ijob['ncpus']):
#                     ijob['has_started'] = 1
#                     ijob['has_started_at'] = t_since_start
#                     avail_nodes = max([avail_nodes - ijob['ncpus'], 0])
#                     print "started job " + ijob['jobID'] + " at t=: " + str(t_since_start) + " avail cpu: ", avail_nodes
#             # ----------------------------------------
#
#             # ------ checks for finished jobs.. ------
#             for ijob in job_list:
#                 if ijob['has_started'] and (t_since_start >= (ijob['has_started_at'] + ijob['runtime'])) and (not ijob['has_finished']):
#                     print "at t=: " + str(t_since_start) + "  job:" + ijob['jobID'] + " has finished" + " avail cpu: ", avail_nodes
#                     ijob['has_finished'] = 1
#                     ijob['has_finished_at'] = t_since_start
#                     avail_nodes = avail_nodes + ijob['ncpus']
#
#                     v1 = ijob['times'] + ijob['has_started_at']
#                     v2 = ijob['IO_N_read_vec']
#                     v3 = ijob['IO_N_write_vec']
#                     vvv = np.vstack((v1, v2, v3))
#                     self.job_output_all = np.hstack(
#                         (self.job_output_all, vvv))
#
#         t_since_start = time.time() - time0
#
#         self.job_output_all = self.job_output_all[
#             :, self.job_output_all[0, :].argsort()]
#         self.job_list = job_list
