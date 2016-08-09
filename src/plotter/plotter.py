import pylab as pylb
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import dates
from datetime import datetime
import csv
import math

from model_workload import ModelWorkload
from logreader.scheduler_reader import PBSDataSet, AccountingDataSet
from tools.print_colour import print_colour
from plot_handler import PlotHandler
import scipy.stats as stats


hour_loc = dates.HourLocator()  # every day
day_loc = dates.DayLocator()  # every day
months_loc = dates.MonthLocator()  # every month
years_loc = dates.YearLocator()  # every year


class Plotter(object):

    """ Plot a dataset """

    def __init__(self, dataset):

        self.dataset = dataset

    def make_plots(self, plot_settings):

        if isinstance(self.dataset, AccountingDataSet) or isinstance(self.dataset, PBSDataSet):

            plot_from_dictionary(plot_settings, self.dataset.joblist)

        else:

            print_colour("orange", "plotter for {} dataset not implemented!".format(type(self.dataset)))


def plot_from_dictionary(plot_settings, list_jobs):

    """ Make plot from dictionary """

    plot_dict = plot_settings['plots']
    run_tag = plot_settings['run_tag']
    plot_out_dir = plot_settings['out_dir']
    csv_keys = ["tag", "title", "queue", "tot num jobs", "Quantity", "max [%]", "max sub-range", "overall range"]

    # take only times after the start time of the first job in the log..
    job0_time_start = list_jobs[np.argmin(np.asarray([i_job.idx_in_log for i_job in list_jobs]))].time_start
    t_date_job0 = datetime.fromtimestamp(job0_time_start)  # convert epoch to float format
    t_date_job0 = dates.date2num(t_date_job0)  # converted

    job_end_time_start = list_jobs[np.argmax(np.asarray([i_job.idx_in_log for i_job in list_jobs]))].time_start
    t_date_job_end = datetime.fromtimestamp(job_end_time_start)  # convert epoch to float format
    t_date_job_end = dates.date2num(t_date_job_end)  # converted

    csv_summary_list = []

    for plot in plot_dict:

        plot_title_all = run_tag + ' - ' + plot['title']
        plot_subplots_list = plot['subplots']

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

                    if ss == 0 and queue_legend!='':
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

                        (t_dates, t_vals_vec) = get_running_vectors(time_start_vec, time_end_vec, cpus_vec)
                        plt.plot(t_dates, t_vals_vec[:, 1], queue_col)

                    subplt_hdl.set_ylim(ymin=0)
                    subplt_hdl.set_ylabel( yname )
                    subplt_hdl.set_xlabel("time")
                    if t_date_job0:
                        subplt_hdl.set_xlim(xmin=t_date_job0, xmax=t_date_job_end)

                    iows_set_xaxis_format(plt.gca(), plot_time_format)

            # ------------- finish plot ---------------
            name_fig = plot_out_dir + "/" + plot_title_all.replace(" ", "_") + "_x=time.png"
            plt.subplot(n_subplots, 1, 1)
            plt.title(plot_title_all)
            fig.tight_layout()

            if lgd_list:
                lgd = plt.legend(lgd_list, loc=2, bbox_to_anchor=(1, 0.5))
                plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
            else:
                plt.savefig(name_fig, bbox_inches='tight')

            plt.close(iFig)
            # ----------------------------------------

        # ----------- titls a histogram plot.. -------------
        elif plot['type'] == 'histogram':

            queue_types = plot['queue_type']
            n_bins = plot['n_bins']

            # # initialize the figure
            # iFig = PlotHandler.get_fig_handle_ID()
            # fig = plt.figure(iFig)
            # lgd_list = []

            n_subplots = len(plot_subplots_list)
            subplt_hdl_list = []

            # get the filtered jobs
            for ss, i_subplot_name in enumerate(plot_subplots_list):

                # initialize the figure
                iFig = PlotHandler.get_fig_handle_ID()
                fig = plt.figure(iFig)
                lgd_list = []

                queue_types = plot['queue_type']
                # subplt_hdl = plt.subplot(n_subplots, 1, ss + 1)
                # subplt_hdl_list.append(subplt_hdl)

                for i_queue in queue_types:

                    queue_label = i_queue[0]
                    queue_legend = i_queue[1]
                    queue_col = i_queue[2]

                    csv_summary = {'tag':run_tag, 'title': plot_title_all, 'queue': queue_legend}

                    # if ss == 0:
                    if queue_legend!='':
                        lgd_list.append(queue_legend)

                    list_jobs_queue = [i_job for i_job in list_jobs if i_job.queue_type in queue_label]

                    if i_subplot_name == 'cpus':
                        y_values = np.asarray([y.ncpus for y in list_jobs_queue])
                        xlabel_name = 'cpus'
                        csv_summary['Quantity'] = 'cpus'
                        hh, bin_edges = np.histogram(y_values, bins=n_bins, density=False)
                        plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=queue_col)
                        # axes
                        subplt_hdl = plt.gca()
                        subplt_hdl.set_xlabel(xlabel_name)
                        subplt_hdl.set_yscale('log')
                        subplt_hdl.set_ylabel('# jobs')

                    elif i_subplot_name == 'nodes':
                        y_values = np.asarray([y.nnodes for y in list_jobs_queue])
                        xlabel_name = 'nodes'
                        csv_summary['Quantity'] = 'nodes'
                        xx_bins = [((2 ** (ii - 1)) + (2 ** (ii))) / 2 for ii in
                                   range(0, int(math.log(max(y_values), 2)) + 3)]
                        xx_bins[0] = 0
                        xx_bins[1] = 1.5
                        xx_bins_labels = [str(int(2 ** i)) for i in range(0, int(math.log(max(y_values), 2)) + 2)]
                        hh, bin_edges = np.histogram(y_values, bins=xx_bins, density=False)

                        x_vec = np.arange(0, len(bin_edges))
                        plt.bar(x_vec[:-1], hh, pylb.diff(x_vec), color=queue_col)

                        subplt_hdl = plt.gca()
                        subplt_hdl.set_xticks(np.arange(0, len(x_vec[:-1])) + 0.5)
                        subplt_hdl.set_xticklabels(xx_bins_labels)

                        # axes
                        subplt_hdl.set_xlabel(xlabel_name)
                        subplt_hdl.set_yscale('log')
                        subplt_hdl.set_ylabel('# jobs')

                    elif i_subplot_name == 'run-time':
                        y_values = np.asarray([y.runtime/3600. for y in list_jobs_queue])
                        xlabel_name = 'run-time [hr]'
                        csv_summary['Quantity'] = 'run-time'
                        hh, bin_edges = np.histogram(y_values, bins=n_bins, density=False)
                        plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=queue_col)
                        # axes
                        subplt_hdl = plt.gca()
                        subplt_hdl.set_xlabel(xlabel_name)
                        subplt_hdl.set_yscale('log')
                        subplt_hdl.set_ylabel('# jobs')

                    elif i_subplot_name == 'queue-time':
                        y_values = np.asarray([y.time_in_queue/3600. for y in list_jobs_queue])
                        xlabel_name = 'queue-time [hr]'
                        csv_summary['Quantity'] = 'queue-time'
                        hh, bin_edges = np.histogram(y_values, bins=n_bins, density=False)
                        plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=queue_col)
                        # axes
                        subplt_hdl = plt.gca()
                        subplt_hdl.set_xlabel(xlabel_name)
                        subplt_hdl.set_yscale('log')
                        subplt_hdl.set_ylabel('# jobs')

                    elif i_subplot_name == 'cpu-hours':
                        y_values = np.asarray([y.runtime/3600.*y.ncpus for y in list_jobs_queue])
                        xlabel_name = 'cpu-hours [#cpu*hr]'
                        csv_summary['Quantity'] = 'cpu-hours'
                        hh, bin_edges = np.histogram(y_values, bins=n_bins, density=False)
                        plt.bar(bin_edges[:-1], hh, pylb.diff(bin_edges), color=queue_col)
                        # axes
                        subplt_hdl = plt.gca()
                        subplt_hdl.set_xlabel(xlabel_name)
                        subplt_hdl.set_yscale('log')
                        subplt_hdl.set_ylabel('# jobs')

                    else:
                        raise KeyError(" subplot type not recognized: {}".format(i_subplot_name))

                    max_value = np.amax(hh)
                    sum_value = np.sum(hh)
                    max_index = np.argmax(hh)
                    csv_summary['max [%]'] = '{:.2f}'.format(max_value/float(sum_value)*100.)
                    csv_summary['max sub-range'] = "[{:.2f},{:.2f}]".format(bin_edges[max_index], bin_edges[max_index+1])
                    csv_summary['overall range'] = "[{:.2f},{:.2f}]".format(bin_edges[0], bin_edges[-1])
                    csv_summary['tot num jobs'] = len(y_values)
                    csv_summary_list.append([csv_summary[x] for x in csv_keys])

                # ------------- finish plot ---------------
                name_fig = plot_out_dir + "/" + plot_title_all.replace(" ", "_") + "_"+i_subplot_name.replace(" ", "_") + "_hist.png"
                # plt.subplot(n_subplots, 1, 1)
                plt.title(plot_title_all + ' histogram of ' + i_subplot_name)
                fig.tight_layout()

                # lgd = plt.legend(lgd_list, loc=2, bbox_to_anchor=(1, 0.5))
                if lgd_list:
                    lgd = plt.legend(lgd_list)
                    plt.savefig(name_fig, bbox_extra_artists=(lgd,), bbox_inches='tight')
                else:
                    plt.savefig(name_fig, bbox_inches='tight')

                plt.close(iFig)
                # ----------------------------------------

    # write histogram summary csv file..
    if csv_summary_list:
        with open(plot_out_dir + "/"+run_tag+'_histograms_summary.csv', 'w') as f:
            w = csv.writer(f)
            w.writerow(csv_keys)
            w.writerows(csv_summary_list)


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


def iows_set_xaxis_format(ax, plot_time_format):

    if plot_time_format in ['%d', '%a']:
        ax.xaxis.set_major_locator( day_loc )
        hfmt = dates.DateFormatter(plot_time_format)
        ax.xaxis.set_major_formatter(hfmt)
        ax.xaxis.set_minor_locator(hour_loc)
        ax.xaxis.set_ticks_position('bottom')

    elif plot_time_format in ['%b', '%m']:
        ax.xaxis.set_major_locator(months_loc)
        hfmt = dates.DateFormatter(plot_time_format)
        ax.xaxis.set_major_formatter(hfmt)
        ax.xaxis.set_ticks_position('bottom')

    elif plot_time_format in ['%Y']:
        ax.xaxis.set_major_locator(years_loc)
        hfmt = dates.DateFormatter(plot_time_format)
        ax.xaxis.set_major_formatter(hfmt)
        ax.xaxis.set_minor_locator(months_loc)
        ax.xaxis.set_ticks_position('bottom')

    else:
        ValueError('dateformat not recognized..')