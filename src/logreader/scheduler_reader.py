from matplotlib import dates

import pylab as pylb
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

import scipy.stats as stats

from tools import *
from plot_handler import PlotHandler
from real_job import RealJob


def read_pbs_logs(filename_in):

    pbs_jobs = []

    log_list_raw = ['ctime',
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
    log_list = ['time_created',
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
                    if yval_val[0] in log_list_raw:
                        # find index
                        idx = log_list_raw.index(yval_val[0])
                        key_name = log_list[idx]
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

            # i_job.cpu_percent = float(line_dict['cpu_percent'].replace(":", ""))
            i_job.group = str(line_dict['group'])
            i_job.jobname = str(line_dict['jobname'])
            i_job.user = str(line_dict['user'])

            pbs_jobs.append(i_job)

            cce += 1
        cc += 1

    # remove invalid entries
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.time_start != -1]
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.time_end != -1]
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.time_end >= i_job.time_start]
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.time_queued != -1]
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.time_start >= i_job.time_queued]
    pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.ncpus > 0]
    pbs_jobs.sort(key=lambda x: x.time_start, reverse=False)

    # times relative to start of log
    min_start_time = min([i_job.time_start for i_job in pbs_jobs])
    for i_job in pbs_jobs:
        i_job.runtime = float(i_job.time_end) - float(i_job.time_start)
        i_job.time_start_0 = i_job.time_start - min_start_time
        i_job.time_in_queue = i_job.time_start - i_job.time_queued

    return pbs_jobs


def make_scheduler_plots(list_jobs, plot_tag, out_dir, date_ticks="month"):
    """make plots"""

    # ncpu(t)
    iFig = PlotHandler.get_fig_handle_ID()
    Fhdl = plt.figure(iFig)
    xname = "time_start_0"
    yname = "ncpus"
    vecXkeys = np.asarray([x.time_start for x in list_jobs])

    # convert epoch to matplotlib float format
    dts = map(datetime.fromtimestamp, vecXkeys)
    fds = dates.date2num(dts)  # converted

    vecYkeys = np.asarray([y.ncpus for y in list_jobs])
    plt.title('x=' + str(xname) + ',  y=' + str(yname))

    plt.plot(fds, vecYkeys, 'k')
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.3)
    ax = plt.gca()

    if date_ticks == "month":
        ax.xaxis.set_major_locator(dates.MonthLocator())
        hfmt = dates.DateFormatter('%Y-%m/%d')
    # elif date_ticks == "hour":
    else:
        ax.xaxis.set_major_locator(dates.HourLocator())
        hfmt = dates.DateFormatter('%m/%d %H:%M')

    ax.xaxis.set_major_formatter(hfmt)
    plt.xticks(rotation='vertical')
    plt.xticks(rotation=90)

    plt.xlabel('date/time')
    plt.ylabel(yname)
    plt.savefig(out_dir + '/' + plot_tag + '_plot_' + 'raw_data' + '_x=' + xname + '_y=' + yname + '.png')
    plt.close(iFig)



    # --------- PDF(ncpus) -----------
    iFig = PlotHandler.get_fig_handle_ID()
    yname = "ncpus"
    vecYkeys = np.asarray([y.ncpus for y in list_jobs])
    nbins = 100
    hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)

    # figure
    plt.figure(iFig)
    plt.title("PDF of ncpus")
    xx = (bin_edges[1:] + bin_edges[:-1]) / 2

    # fit gamma distribution
    fit_shape, fit_loc, fit_scale = stats.gamma.fit(vecYkeys)
    gg_pdf = stats.gamma.pdf(xx, fit_shape, fit_loc, fit_scale)

    # plotting..
    line1 = plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color='b')
    line2, = plt.plot(xx, gg_pdf, 'k-')
    lgd = plt.legend([line1, line2],
               ['raw data', 'PDF gamma model:\nk=%.3f\nl=%.3f\nsc=%.3f'%(fit_shape, fit_loc, fit_scale)],
               loc=2,
               bbox_to_anchor=(1, 0.5))

    plt.yscale('log')
    plt.xlabel('N CPU''s')
    plt.savefig(out_dir + '/' + plot_tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png',
                bbox_extra_artists=(lgd,),
                bbox_inches='tight')
    plt.close(iFig)

    # ------------ PDF(memory Kb) ------------
    # iFig = PlotHandler.get_fig_handle_ID()
    # yname  = "memory_kb"
    # vecYkeys = np.asarray( [ y.memory_kb  for y in list_jobs ] )
    # nbins = 40
    # hh,bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)
    # figure( iFig )
    # title("PDF of memory_kb")
    # xx = (bin_edges[1:] + bin_edges[:-1]) / 2
    # bar( bin_edges[:-1] , hh, diff(bin_edges), color='r')
    # yscale('log')
    # xlabel( 'Average job memory [Kb]' )
    # savefig(out_dir+'/'+plot_tag+'_plot_'+'raw_data'+'_y='+yname+'_hist.png')
    # close(iFig)

    # --------- PDF(runtime) -----------
    iFig = PlotHandler.get_fig_handle_ID()
    yname = "runtime"
    vecYkeys = np.asarray([y.runtime for y in list_jobs])
    nbins = 100
    hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)

    # figure
    plt.figure(iFig)
    plt.title("PDF of runtime")
    xx = (bin_edges[1:] + bin_edges[:-1]) / 2

    # fit gamma distribution
    fit_shape, fit_loc, fit_scale = stats.gamma.fit(vecYkeys)
    gg_pdf = stats.gamma.pdf(xx, fit_shape, fit_loc, fit_scale)

    # plotting..
    line1 = plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color='r')
    line2, = plt.plot(xx, gg_pdf, 'k-')
    lgd = plt.legend([line1, line2],
                     ['raw data', 'PDF gamma model:\nk=%.3f\nl=%.3f\nsc=%.3f' % (fit_shape, fit_loc, fit_scale)],
                     loc=2,
                     bbox_to_anchor=(1, 0.5))

    plt.yscale('log')
    plt.xlabel('Job runtime [s]')
    plt.savefig(out_dir + '/' + plot_tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png',
                bbox_extra_artists=(lgd,),
                bbox_inches='tight')
    plt.close(iFig)

    # --------- PDF(qtime) -----------
    iFig = PlotHandler.get_fig_handle_ID()
    yname = "Queuing time"
    vecYkeys = np.asarray([y.time_in_queue for y in list_jobs])
    nbins = 100
    hh, bin_edges = np.histogram(vecYkeys, bins=nbins, density=True)

    # figure
    plt.figure(iFig)
    plt.title("PDF of Queuing time")
    xx = (bin_edges[1:] + bin_edges[:-1]) / 2

    # fit gamma distribution
    fit_shape, fit_loc, fit_scale = stats.gamma.fit(vecYkeys)
    gg_pdf = stats.gamma.pdf(xx, fit_shape, fit_loc, fit_scale)

    # plotting..
    line1 = plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color='g')
    line2, = plt.plot(xx, gg_pdf, 'k-')
    lgd = plt.legend([line1, line2],
                     ['raw data', 'PDF gamma model:\nk=%.3f\nl=%.3f\nsc=%.3f' % (fit_shape, fit_loc, fit_scale)],
                     loc=2,
                     bbox_to_anchor=(1, 0.5))

    plt.yscale('log')
    plt.xlabel('Job queuing time [s]')
    plt.savefig(out_dir + '/' + plot_tag + '_plot_' + 'raw_data' + '_y=' + yname + '_hist.png',
                bbox_extra_artists=(lgd,),
                bbox_inches='tight')
    plt.close(iFig)
