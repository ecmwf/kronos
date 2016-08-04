import os

from matplotlib import dates
import csv

import pylab as pylb
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

import scipy.stats as stats

from exceptions_iows import ConfigurationError
from logreader.dataset import IngestedDataSet
from tools import *
from plot_handler import PlotHandler
from jobs import IngestedJob, ModelJob


def read_pbs_log(filename_in):

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
                    'Resource_List.EC_nodes',
                    'queue'
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
                'nnodes',
                'queue'
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
                            line_dict["ncpus"] = int(yval_val[2]) * int(yval_val[1].split(":")[0])
                    else:
                        line_dict["ncpus"] = int(yval_val[1])

            i_job = IngestedJob()

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

            if 'nnodes' in line_dict:
                i_job.nnodes = int(line_dict['nnodes'])
            else:
                i_job.nnodes = -1

            # i_job.cpu_percent = float(line_dict['cpu_percent'].replace(":", ""))
            i_job.group = str(line_dict['group'])
            i_job.jobname = str(line_dict['jobname'])
            i_job.user = str(line_dict['user'])
            i_job.queue_type = str(line_dict['queue'])

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
    # pbs_jobs[:] = [i_job for i_job in pbs_jobs if i_job.nnodes > 0]

    for (ii, i_job) in enumerate(pbs_jobs):
        i_job.idx_in_log = ii

    pbs_jobs.sort(key=lambda x: x.time_start, reverse=False)

    # times relative to start of log
    min_start_time = min([i_job.time_start for i_job in pbs_jobs])
    for i_job in pbs_jobs:
        i_job.runtime = float(i_job.time_end) - float(i_job.time_start)
        i_job.time_start_0 = i_job.time_start - min_start_time
        i_job.time_in_queue = i_job.time_start - i_job.time_queued

    return pbs_jobs


def read_accounting_logs(filename_in):

    accounting_jobs = []

    # ['"hpc"',
    #  '"jobid"',
    #  '"jobname"',
    #  '"jobstepid"',
    #  '"owner_uid"',
    #  '"owner_group"',
    #  '"submitter_uid"',
    #  '"submitter_group"',
    #  '"queue_time"',
    #  '"start_time"',
    #  '"end_time"',
    #  '"no_nodes"',
    #  '"no_cpus"',
    #  '"class"',
    #  '"account"',
    #  '"usage"',
    #  '"sbu"',
    #  '"step_usertime"',
    #  '"stdin"',
    #  '"stdout"',
    #  '"stderr"',
    #  '"experiment_id"']

    with open(filename_in, 'rb') as csvfile:

        csv_dict = csv.DictReader(csvfile, delimiter=';', quotechar='"')

        for line_dict in csv_dict:

            i_job = IngestedJob()

            try:
                i_job.time_queued = (datetime.strptime(line_dict['queue_time'], '%Y-%m-%d %H:%M:%S') -
                                     datetime(1970, 1, 1)).total_seconds()
            except ValueError:
                i_job.time_queued = (datetime.strptime(line_dict['queue_time'], '%Y-%m-%d %H:%M:%S.%f') -
                                     datetime(1970, 1, 1)).total_seconds()

            try:
                i_job.time_end = (datetime.strptime(line_dict['end_time'], '%Y-%m-%d %H:%M:%S.%f') -
                                  datetime(1970, 1, 1)).total_seconds()
            except ValueError:
                i_job.time_end = (datetime.strptime(line_dict['end_time'], '%Y-%m-%d %H:%M:%S') -
                                  datetime(1970, 1, 1)).total_seconds()

            try:
                i_job.time_start = (datetime.strptime(line_dict['start_time'], '%Y-%m-%d %H:%M:%S.%f') -
                                    datetime(1970, 1, 1)).total_seconds()
            except ValueError:
                i_job.time_start = (datetime.strptime(line_dict['start_time'], '%Y-%m-%d %H:%M:%S') -
                                    datetime(1970, 1, 1)).total_seconds()

            if 'no_cpus' in line_dict:
                i_job.ncpus = int(line_dict['no_cpus'])
            else:
                i_job.ncpus = -1

            if 'no_nodes' in line_dict:
                i_job.nnodes = int(line_dict['no_nodes'])
            else:
                i_job.nnodes = -1

            # i_job.cpu_percent = float(line_dict['cpu_percent'].replace(":", ""))
            i_job.group = str(line_dict['owner_group'])
            i_job.jobname = str(line_dict['jobname'])
            i_job.user = str(line_dict['owner_uid'])
            i_job.queue_type = str(line_dict['class'])

            # print i_job.queue_type

            # info not available
            i_job.time_created = -1
            i_job.time_eligible = -1
            i_job.memory_kb = -1

            accounting_jobs.append(i_job)

    # remove invalid entries
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.time_start != -1]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.time_end != -1]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.time_end >= i_job.time_start]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.time_queued != -1]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.time_start >= i_job.time_queued]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.ncpus > 0]
    accounting_jobs[:] = [i_job for i_job in accounting_jobs if i_job.nnodes > 0]

    # store the original idx of each job..
    for (ii, i_job) in enumerate(accounting_jobs):
        i_job.idx_in_log = ii

    accounting_jobs.sort(key=lambda x: x.time_start, reverse=False)

    # times relative to start of log
    min_start_time = min([i_job.time_start for i_job in accounting_jobs])
    for i_job in accounting_jobs:
        # print type(i_job.time_queued), type(i_job.time_end), type(i_job.time_start)
        i_job.runtime = float(i_job.time_end) - float(i_job.time_start)
        i_job.time_start_0 = i_job.time_start - min_start_time
        i_job.time_in_queue = i_job.time_start - i_job.time_queued

    return accounting_jobs


def read_epcc_csv_logs(filename_in):

    """ read CSV logs from EPCC.. """

    csv_jobs = []

    with open(filename_in, 'rb') as csvfile:

        csv_dict = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for line_dict in csv_dict:

            i_job = IngestedJob()

            # if isinstance(line_dict['ctime'], str):
            #     # i_job.time_queued = int(line_dict['ctime'])
            #     i_job.time_queued = int(line_dict['start']) + 999  # will be removed later..
            # else:
            #     print "line_dict['ctime']: ", line_dict['ctime']
            #     i_job.time_queued = int(line_dict['start']) + 999 #  will be removed later..

            try:
                i_job.time_queued = int(line_dict['ctime'])
            except:
                print("I didn't recognize ctime {0} as a number".format(line_dict['ctime']))
                i_job.time_queued = -1

            try:
                i_job.time_end = int(line_dict['end'])
            except:
                print("I didn't recognize end {0} as a number".format(line_dict['end']))
                i_job.time_end = -1

            try:
                i_job.time_start = int(line_dict['start'])
            except:
                print("I didn't recognize start {0} as a number".format(line_dict['start']))
                i_job.time_start = -1

            try:
                i_job.ncpus = int(line_dict['ncpus'])
            except:
                print("I didn't recognize start {0} as a number".format(line_dict['ncpus']))
                i_job.ncpus = -1

            try:
                i_job.nnodes = int(line_dict['node_count'])
            except:
                print("I didn't recognize start {0} as a number".format(line_dict['node_count']))
                i_job.nnodes = -1

            # i_job.group = line_dict['group'].strip()
            i_job.group = ''

            if line_dict['jobname']:
                i_job.jobname = line_dict['jobname'].strip()
            else:
                i_job.jobname = ''

            if line_dict['jobname']:
                i_job.user = line_dict['UserID'].strip()
            else:
                i_job.user = ''

            if line_dict['jobname']:
                i_job.queue_type = line_dict['queue'].strip()
            else:
                i_job.queue_type = ''

            # info not available
            i_job.time_created = -1
            i_job.time_eligible = -1
            i_job.memory_kb = -1

            csv_jobs.append(i_job)

    # remove invalid entries
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.time_start != -1]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.time_end != -1]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.time_end >= i_job.time_start]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.time_queued != -1]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.time_start >= i_job.time_queued]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.ncpus > 0]
    csv_jobs[:] = [i_job for i_job in csv_jobs if i_job.nnodes > 0]

    # store the original idx of each job..
    for (ii, i_job) in enumerate(csv_jobs):
        i_job.idx_in_log = ii

    csv_jobs.sort(key=lambda x: x.time_start, reverse=False)

    # times relative to start of log
    min_start_time = min([i_job.time_start for i_job in csv_jobs])
    for i_job in csv_jobs:
        # print type(i_job.time_queued), type(i_job.time_end), type(i_job.time_start)
        i_job.runtime = float(i_job.time_end) - float(i_job.time_start)
        i_job.time_start_0 = i_job.time_start - min_start_time
        i_job.time_in_queue = i_job.time_start - i_job.time_queued

    return csv_jobs


class PBSDataSet(IngestedDataSet):

    def __init__(self, joblist, *args, **kwargs):
        super(PBSDataSet, self).__init__(joblist, *args, **kwargs)

        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        created_time_list = [j.time_created for j in self.joblist if j.time_created >= 0]
        self.global_created_time = 0.0
        if created_time_list:
            self.global_created_time = min(created_time_list)

        start_time_list = [j.time_created for j in self.joblist if j.time_created >= 0]
        self.global_start_time = 0.0
        if start_time_list:
            self.global_start_time = min(start_time_list)

    def model_jobs(self):
        for job in self.joblist:
            assert isinstance(job, IngestedJob)
            assert not job.timesignals

            if job.time_created >= 0:
                submit_time = job.time_created - self.global_created_time
            else:
                submit_time = job.time_start - self.global_start_time

            yield ModelJob(
                time_start=submit_time,
                duration=job.time_end-job.time_start,
                ncpus=job.ncpus,
                nnodes=job.nnodes
            )


def ingest_pbs_logs(path):
    """
    Read PBS logs into a dataset
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest PBS profiles does not exist: {}".format(path))

    if not os.path.isfile(path):
        raise ConfigurationError("Specified path for PBS schedule is not a file")

    jobs = read_pbs_log(path)

    return PBSDataSet(jobs)


def ingest_epcc_csv_logs(path):
    """
    Read PBS logs into a dataset
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest CSV profiles does not exist: {}".format(path))

    if not os.path.isfile(path):
        raise ConfigurationError("Specified path for CSV schedule is not a file")

    jobs = read_epcc_csv_logs(path)

    return PBSDataSet(jobs)


class AccountingDataSet(IngestedDataSet):

    def __init__(self, joblist, *args, **kwargs):
        super(AccountingDataSet, self).__init__(joblist, *args, **kwargs)

        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        self.global_start_time = min((j.time_start for j in self.joblist if j.time_start >= 0))

    def model_jobs(self):
        for job in self.joblist:
            assert isinstance(job, IngestedJob)
            assert not job.timesignals

            yield ModelJob(
                time_start=job.time_start - self.global_start_time,
                duration=job.time_end-job.time_start,
                ncpus=job.ncpus,
                nnodes=job.nnodes
            )


def ingest_accounting_logs(path):
    """
    Read PBS logs into a dataset
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest accounting profiles does not exist: {}".format(path))

    if not os.path.isfile(path):
        raise ConfigurationError("Specified path for accounting log is not a file")

    jobs = read_accounting_logs(path)

    return PBSDataSet(jobs)


def make_scheduler_plots(list_jobs, plot_tag, out_dir, plot_dict=None, date_ticks="month"):

    """ prepare plots"""

    if not plot_dict:

        pass

        # if plot_tag == "ECMWF":
        #
        #     # fractional jobs
        #     list_jobs_fractional = [i_job for i_job in list_jobs if i_job.queue_type in ['ns', 'nf', 'of', 'os']]
        #     plotter(list_jobs_fractional, "fractional", plot_tag, out_dir, date_ticks)
        #
        #     # parallel jobs
        #     list_jobs_parallel = [i_job for i_job in list_jobs if i_job.queue_type in ['np', 'op']]
        #     plotter(list_jobs_parallel, "parallel", plot_tag, out_dir, date_ticks)
        #
        #     # additional plot zooming on the Friday..
        #     list_jobs_friday = [i_job for i_job in list_jobs if datetime.fromtimestamp(i_job.time_start).strftime("%A") == "Friday"]
        #     list_jobs_parallel = [i_job for i_job in list_jobs_friday if i_job.queue_type in ['np', 'op']]
        #     plotter(list_jobs_parallel, "parallel", plot_tag+" 1-day", out_dir, "hour")
        #
        # if plot_tag == "ARCTUR":
        #
        #     # all jobs..
        #     plotter(list_jobs, "fractional", plot_tag, out_dir, date_ticks)
        #
        # if plot_tag == "EPCC":
        #
        #     # fractional jobs
        #     list_jobs_fractional = [i_job for i_job in list_jobs if i_job.queue_type in ['serial']]
        #     plotter(list_jobs_fractional, "fractional", plot_tag, out_dir, date_ticks)
        #
        #     # parallel jobs
        #     list_jobs_parallel = [i_job for i_job in list_jobs if i_job.queue_type in ['standard', 'low']]
        #     plotter(list_jobs_parallel, "parallel", plot_tag, out_dir, date_ticks)

    else:

        plot_from_dictionary(plot_dict, list_jobs)


def get_running_vectors(ts, te, vals_vec):

    # reshape t_start, time_end, y_valuse
    time_start_vec = ts.reshape(len(ts), 1)
    time_end_vec = te.reshape(len(te), 1)
    y_vec = vals_vec.reshape(len(vals_vec), 1)

    # calculate #jobs running
    ts_vec = np.hstack((time_start_vec, y_vec))
    te_vec = np.hstack((time_end_vec, (-1.) * y_vec))
    t_ones_vec = np.vstack((ts_vec, te_vec))
    t_ones_vec = t_ones_vec[t_ones_vec[:, 0].argsort(), :]
    cum_sum_jobs = np.cumsum(t_ones_vec[:, 1])
    cum_sum_jobs = cum_sum_jobs.reshape(len(cum_sum_jobs), 1)
    t_vals_vec = np.hstack((t_ones_vec[:, 0].reshape(len(t_ones_vec[:, 0]), 1), cum_sum_jobs))

    # convert epochs to dates
    t_dates = map(datetime.fromtimestamp, t_vals_vec[:, 0])  # convert epoch to float format
    t_dates = dates.date2num(t_dates)  # converted

    return (t_dates, t_vals_vec)


def plot_from_dictionary(plot_dict, list_jobs):

    """ plot from dictionary """

    # take only times after the start time of the first job in the log..
    job0_time_start = list_jobs[np.argmin(np.asarray([i_job.idx_in_log for i_job in list_jobs]))].time_start
    t_date_job0 = datetime.fromtimestamp(job0_time_start)  # convert epoch to float format
    t_date_job0 = dates.date2num(t_date_job0)  # converted

    job_end_time_start = list_jobs[np.argmax(np.asarray([i_job.idx_in_log for i_job in list_jobs]))].time_start
    t_date_job_end = datetime.fromtimestamp(job_end_time_start)  # convert epoch to float format
    t_date_job_end = dates.date2num(t_date_job_end)  # converted

    for plot in plot_dict:

        plot_title = plot['title']
        plot_subplots_list = plot['subplots']
        plot_time_format = plot['time format']
        plot_out_dir = plot['out_dir']

        # initialize the figure
        iFig = PlotHandler.get_fig_handle_ID()
        Fhdl = plt.figure(iFig)
        lgd_list = []

        n_subplots = len(plot_subplots_list)

        for ss, i_subplot_name in enumerate(plot_subplots_list):

            yname = i_subplot_name
            queue_types = plot['queue_type']
            plt.subplot(n_subplots, 1, ss+1)

            for i_queue in queue_types:

                queue_label = i_queue[0]
                queue_legend = i_queue[1]
                queue_col = i_queue[2]

                if ss == 0:
                    lgd_list.append(queue_legend)

                list_jobs_queue = [i_job for i_job in list_jobs if i_job.queue_type in queue_label]

                # take relevant vectors..
                time_start_vec = np.asarray([i_job.time_start for i_job in list_jobs_queue])
                time_end_vec = np.asarray([i_job.time_end for i_job in list_jobs_queue])

                cpus_vec = np.asarray([i_job.ncpus for i_job in list_jobs_queue])
                node_vec = np.asarray([i_job.nnodes for i_job in list_jobs_queue])
                jobs_vec = np.ones(len(cpus_vec))

                if i_subplot_name == 'jobs':

                    (t_dates, t_vals_vec) = get_running_vectors(time_start_vec, time_end_vec, jobs_vec)
                    plt.plot(t_dates, t_vals_vec[:, 1], queue_col)

                elif i_subplot_name == 'nodes':

                    (t_dates, t_vals_vec) = get_running_vectors(time_start_vec, time_end_vec, node_vec)
                    plt.plot(t_dates, t_vals_vec[:, 1], queue_col)

                elif i_subplot_name == 'cpus':

                    (t_dates, t_vals_vec) = get_running_vectors(time_start_vec, time_end_vec, node_vec)
                    plt.plot(t_dates, t_vals_vec[:, 1], queue_col)

                plt.ylim(ymin=0)
                plt.ylabel( yname )
                if t_date_job0:
                    plt.xlim(xmin=t_date_job0, xmax=t_date_job_end)

                ax = plt.gca()
                ax.xaxis.set_major_locator(dates.MonthLocator())
                hfmt = dates.DateFormatter(plot_time_format)
                ax.xaxis.set_major_formatter(hfmt)

        # ------------- finish plot ---------------
        name_fig = plot_out_dir + "/" + plot_title.replace(" ", "_") + "_x=time.png"
        plt.subplot(n_subplots, 1, 1)
        plt.title(plot_title)
        lgd = plt.legend(lgd_list, loc=2, bbox_to_anchor=(1, 0.5))
        plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
        plt.close(iFig)
        # ----------------------------------------






    #
    #
    #
    #
    # # -----------------------  PDF plots -------------------------
    # if "ECMWF" in plot_tag:
    #     list_jobs_op = [i_job for i_job in list_jobs if (i_job.queue_type[0] == "o" and i_job.queue_type != "opencl")]
    #     list_jobs_normal = [i_job for i_job in list_jobs if (i_job.queue_type == "np")]
    #     plot_data_list = [
    #         ["ncpus", np.asarray([y.ncpus for y in list_jobs_normal]), 'b'],
    #         ["Runtime [hr]", np.asarray([y.runtime / 3600. for y in list_jobs_normal]), 'r'],
    #         ["Queuing time [hr]", np.asarray([y.time_in_queue / 3600. for y in list_jobs_normal]), 'g'],
    #         ["ncpus (operational)", np.asarray([y.ncpus for y in list_jobs_op]), 'b'],
    #         ["Runtime [hr] (operational)", np.asarray([y.runtime / 3600. for y in list_jobs_op]), 'r'],
    #         ["Queuing time [hr] (operational)", np.asarray([y.time_in_queue / 3600. for y in list_jobs_op]), 'g'],
    #     ]
    # elif ("ARCTUR" in plot_tag) or ("EPCC" in plot_tag):
    #     list_jobs_normal = list_jobs
    #     plot_data_list = [
    #         ["ncpus", np.asarray([y.ncpus for y in list_jobs_normal]), 'b'],
    #         ["Runtime [hr]", np.asarray([y.runtime / 3600. for y in list_jobs_normal]), 'r'],
    #         ["Queuing time [hr]", np.asarray([y.time_in_queue / 3600. for y in list_jobs_normal]), 'g'],
    #     ]
    #
    # print "------------- " + queue_type_plot_tag
    #
    # n_bins = 30
    # for (iPDF, name_y_values) in enumerate(plot_data_list):
    #
    #     yname = name_y_values[0]
    #
    #     yname_file = yname.replace(" ", "-").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    #     y_value_list = name_y_values[1]
    #
    #     # # print y_value_list
    #     # print type(y_value_list)
    #     #
    #     # if isinstance(y_value_list, list):
    #     #     # print plot_data_list
    #     #     pass
    #
    #     if y_value_list.size:
    #
    #         print yname, " max=", max(y_value_list), " min=", min(y_value_list)
    #
    #         n_y_values = len(y_value_list)
    #         col = name_y_values[2]
    #
    #         iFig = PlotHandler.get_fig_handle_ID()
    #         hh, bin_edges = np.histogram(y_value_list, bins=n_bins, density=False)
    #         # hh, bin_edges = np.histogram(y_value_list, bins='auto', density=False)
    #         # histogram_scaling = sum(hh) * (bin_edges[1] - bin_edges[0])
    #
    #         # figure
    #         plt.figure(iFig)
    #         plt.title(yname + " - #Jobs: " + str(n_y_values) + " " + queue_type_plot_tag)
    #         xx = (bin_edges[1:] + bin_edges[:-1]) / 2
    #
    #         # # fit gamma distribution
    #         # fit_shape, fit_loc, fit_scale = stats.gamma.fit(y_value_list)
    #         # gg_pdf = stats.gamma.pdf(xx, fit_shape, fit_loc, fit_scale)
    #
    #         # # plotting..
    #         # line1 = plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=col)
    #         # line2, = plt.plot(bin_edges[:-1], gg_pdf * histogram_scaling, 'k-')
    #         # lgd = plt.legend([line1, line2],
    #         #                  ['raw data', 'Gamma model:\nk=%.3f\nl=%.3f\nsc=%.3f' % (fit_shape, fit_loc, fit_scale)],
    #         #                  loc=2,
    #         #                  bbox_to_anchor=(1, 0.5))
    #
    #         # plotting..
    #         plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=col)
    #         # print yname_file + '_' + queue_type_plot_tag + ': ', bin_edges[:5]
    #
    #         plt.yscale('log')
    #         plt.xlabel(yname)
    #         plt.xlim([0, max(bin_edges)*1.1])
    #
    #         name_fig = out_dir + '/' + plot_tag + '_plot_' + '_y=' + yname_file + '_' + queue_type_plot_tag + '_pdf.png'
    #         name_fig = name_fig.replace(" ", "_")
    #         # plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
    #         plt.savefig(name_fig)
    #         plt.close(iFig)
    #
    #         # --------- save CSV file ------------
    #         list_dict_y = []
    #         for xx, yy in zip(xx, hh):
    #             dd = {yname: xx, "N": yy}
    #             list_dict_y.append(dd)
    #
    #         list_dict_y_sorted = sorted(list_dict_y, key=lambda k: k[yname])
    #         # list_dict_y_sorted = sorted(list_dict_y, key=lambda k: k[yname], reverse=True)
    #         f = open(name_fig + ".csv", 'w')
    #         w = csv.DictWriter(f, [yname, "N"])
    #         w.writeheader()
    #         w.writerows(list_dict_y_sorted)
    #         f.close()
    #         # --------------------------------------------------------------
