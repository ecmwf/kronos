import pylab as pylb
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import dates
import csv
from datetime import datetime

from model_workload import ModelWorkload
from logreader.scheduler_reader import PBSDataSet, AccountingDataSet
from tools.print_colour import print_colour
from plot_handler import PlotHandler
import scipy.stats as stats


class Plotter(object):

    """ Plot a dataset """

    def __init__(self, dataset):

        self.dataset = dataset

    def make_plots(self, plot_dict):

        if isinstance(self.dataset, AccountingDataSet) or isinstance(self.dataset, PBSDataSet):
            plot_from_dictionary(plot_dict, self.dataset.joblist)
        else:
            print_colour("orange", "plotter for {} dataset not implemented!".format(type(self.dataset)))


def plot_from_dictionary(plot_dict, list_jobs):

    """ Make plot from dictionary """

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
        plot_out_dir = plot['out_dir']

        # ----------- titls a time series plot.. -------------
        if plot['type'] == 'time series':

            plot_time_format = plot['time format']

            # initialize the figure
            iFig = PlotHandler.get_fig_handle_ID()
            fig = plt.figure(iFig)
            lgd_list = []

            n_subplots = len(plot_subplots_list)

            for ss, i_subplot_name in enumerate(plot_subplots_list):

                yname = i_subplot_name
                queue_types = plot['queue_type']
                subplt_hdl = plt.subplot(n_subplots, 1, ss+1)

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

                    subplt_hdl.set_ylim(ymin=0)
                    subplt_hdl.set_ylabel( yname )
                    subplt_hdl.set_xlabel("time")
                    if t_date_job0:
                        subplt_hdl.set_xlim(xmin=t_date_job0, xmax=t_date_job_end)

                    ax = plt.gca()
                    ax.xaxis.set_major_locator(dates.MonthLocator())
                    hfmt = dates.DateFormatter(plot_time_format)
                    ax.xaxis.set_major_formatter(hfmt)

            # ------------- finish plot ---------------
            name_fig = plot_out_dir + "/" + plot_title.replace(" ", "_") + "_x=time.png"
            plt.subplot(n_subplots, 1, 1)
            plt.title(plot_title)
            fig.tight_layout()
            lgd = plt.legend(lgd_list, loc=2, bbox_to_anchor=(1, 0.5))
            plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
            plt.close(iFig)
            # ----------------------------------------

        # ----------- titls a histogram plot.. -------------
        elif plot['type'] == 'histogram':

            queue_types = plot['queue_type']
            n_bins = plot['n_bins']

            # initialize the figure
            iFig = PlotHandler.get_fig_handle_ID()
            fig = plt.figure(iFig)
            lgd_list = []

            n_subplots = len(plot_subplots_list)
            subplt_hdl_list = []

            # get the filtered jobs
            for ss, i_subplot_name in enumerate(plot_subplots_list):

                yname = i_subplot_name
                queue_types = plot['queue_type']
                subplt_hdl = plt.subplot(n_subplots, 1, ss + 1)
                subplt_hdl_list.append(subplt_hdl)

                for i_queue in queue_types:

                    queue_label = i_queue[0]
                    queue_legend = i_queue[1]
                    queue_col = i_queue[2]

                    if ss == 0:
                        lgd_list.append(queue_legend)

                    list_jobs_queue = [i_job for i_job in list_jobs if i_job.queue_type in queue_label]

                    if i_subplot_name == 'ncpus':
                        y_values = np.asarray([y.ncpus for y in list_jobs_queue])

                    elif i_subplot_name == 'run-time':
                        y_values = np.asarray([y.runtime for y in list_jobs_queue])

                    elif i_subplot_name == 'queue-time':
                        y_values = np.asarray([y.time_in_queue for y in list_jobs_queue])

                    else:
                        raise KeyError(" subplot type ['ncpus', 'runtime' or 'queuetime'] not recognized: {}".format(i_subplot_name))

                    iFig = PlotHandler.get_fig_handle_ID()
                    hh, bin_edges = np.histogram(y_values, bins=n_bins, density=False)
                    plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=queue_col)

                    subplt_hdl.set_xlabel(i_subplot_name)
                    subplt_hdl.set_yscale('log')
                    subplt_hdl.set_ylabel('# jobs')

            # ------------- finish plot ---------------
            name_fig = plot_out_dir + "/" + plot_title.replace(" ", "_") + "_x=time.png"
            plt.subplot(n_subplots, 1, 1)
            plt.title(plot_title)
            fig.tight_layout()
            lgd = plt.legend(lgd_list, loc=2, bbox_to_anchor=(1, 0.5))
            plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
            plt.close(iFig)
            # ----------------------------------------

            # list_dict_y = []
            # for xx, yy in zip(xx, hh):
            #     dd = {yname: xx, "N": yy}
            #     list_dict_y.append(dd)
            # list_dict_y_sorted = sorted(list_dict_y, key=lambda k: k[yname])
            # # list_dict_y_sorted = sorted(list_dict_y, key=lambda k: k[yname], reverse=True)
            # f = open(name_fig + ".csv", 'w')
            # w = csv.DictWriter(f, [yname, "N"])
            # w.writeheader()
            # w.writerows(list_dict_y_sorted)
            # f.close()


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

    return t_dates, t_vals_vec