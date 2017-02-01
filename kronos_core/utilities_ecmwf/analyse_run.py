#!/usr/bin/env python2.7
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Kronos tool to analise the results of a run
"""

import argparse
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm

from plot_handler import PlotHandler
from workload_data import WorkloadData
from kronos_io.profile_format import ProfileFormat

if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path_kpf", type=str, help="The path of the KPF file to read")
    parser.add_argument("-p", "--plot", action="store_true", help="plot the output of the run")
    parser.add_argument("-s", "--summary", action="store_true", help="make a summary of the run")
    args = parser.parse_args()

    # ------- retrieve kpf data from iteration ----------
    with open(args.path_kpf, 'r') as f:
        kpf_workload = ProfileFormat.from_file(f).workload()

    tot_n_apps = len(kpf_workload.jobs)
    labels_list = set(j.label for j in kpf_workload.jobs)
    labels_jobs_dict = {}
    for lab in labels_list:
        for j in kpf_workload.jobs:
            if j.label == lab:
                if labels_jobs_dict.get(lab, None):
                    labels_jobs_dict[lab].append(j)
                else:
                    labels_jobs_dict[lab] = [j]

    # global properties of the whole workload
    total_t_min = min(sa.time_start for sa in kpf_workload.jobs)
    total_t_max = max(sa.time_start+sa.duration for sa in kpf_workload.jobs)
    total_exe_time = total_t_max - total_t_min

    global_t0 = kpf_workload.min_time_start

    # queuing times of jobs..
    queuing_times_vec = [j.time_queued for j in kpf_workload.jobs]
    queuing_times_vec.sort()
    queuing_times_vec = np.asarray(queuing_times_vec)
    queuing_times_vec = queuing_times_vec - queuing_times_vec[0]

    if args.plot:

        plt_hdl = PlotHandler()
        color = iter(cm.rainbow(np.linspace(0, 1, len(labels_list))))

        # ---------- make plot ----------
        n_plots = len(kpf_workload.total_metrics_timesignals.keys()) + 3
        fig_size = (16, 3 * n_plots)
        plt.figure(plt_hdl.get_fig_handle_ID(), figsize=fig_size)
        id_plot = 0

        for ll, label in enumerate(labels_jobs_dict.keys()):

            label_jobs = labels_jobs_dict[label]
            kpf_workload = WorkloadData(jobs=label_jobs,
                                        tag=label)

            line_color = next(color)

            # calculate running jobs in time..
            start_time_vec = np.asarray([sa.time_start for sa in kpf_workload.jobs])
            start_time_vec = start_time_vec.reshape(start_time_vec.shape[0], 1)
            plus_vec = np.hstack((start_time_vec, np.ones(start_time_vec.shape)))

            end_time_vec = np.asarray([sa.time_start+sa.duration for sa in kpf_workload.jobs])
            end_time_vec = end_time_vec.reshape(end_time_vec.shape[0], 1)
            minus_vec = np.hstack((end_time_vec, -1 * np.ones(end_time_vec.shape)))

            # vector of time-stamps (and +-1 for start-end time stamps)
            all_vec = np.vstack((plus_vec, minus_vec))
            all_vec_sort = all_vec[all_vec[:, 0].argsort()]
            n_running_vec = np.cumsum(all_vec_sort[:, 1])

            # calculate used procs in time..
            proc_time_vec = np.asarray([sa.ncpus for sa in kpf_workload.jobs])
            proc_time_vec = proc_time_vec.reshape(proc_time_vec.shape[0], 1)
            plus_vec = np.hstack((start_time_vec, proc_time_vec))
            minus_vec = np.hstack((end_time_vec, -1 * proc_time_vec))

            all_vec = np.vstack((plus_vec, minus_vec))
            all_vec_sort = all_vec[all_vec[:, 0].argsort()]
            nproc_running_vec = np.cumsum(all_vec_sort[:, 1])

            time_stamps = all_vec_sort[:, 0] - all_vec_sort[0, 0]

            # initial plot with the running jobs/processes
            id_plot = 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            plt.plot(queuing_times_vec, np.arange(len(queuing_times_vec))+1,
                     color=line_color,
                     label=label)
            plt.ylabel('#queued jobs')
            plt.xlim(xmin=0, xmax=total_exe_time)
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            plt.title('Time profiles, Number of apps: {}'.format(tot_n_apps))

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            plt.plot(time_stamps,
                     n_running_vec,
                     color=line_color,
                     label=label)
            plt.ylabel('#jobs')
            plt.xlim(xmin=0, xmax=total_exe_time)

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            plt.plot(time_stamps,
                     nproc_running_vec,
                     color=line_color,
                     label=label)
            plt.ylabel('#procs')
            plt.xlim(xmin=0, xmax=total_exe_time)

            # plot of the total time-signals
            total_metrics = kpf_workload.total_metrics_timesignals
            for tt, ts_name in enumerate(total_metrics):

                id_plot += 1
                plt.subplot(n_plots, 1, id_plot)
                plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
                plt.plot(kpf_workload.total_metrics_timesignals[ts_name].xvalues - global_t0,
                         kpf_workload.total_metrics_timesignals[ts_name].yvalues,
                         color=line_color,
                         label=label)

                plt.ylabel(ts_name)
                plt.xlim(xmin=0, xmax=total_exe_time)

                if tt == len(total_metrics)-1:
                    plt.xlabel('time')

        # plt.savefig(os.path.join(self.run_dir, 'time_profiles_iter-{}.png'.format(i_iter)))
        # plt.close()
        plt.show()

    elif args.summary:
        print "Run summary not yet implemented.."

    else:
        print "Nothing done! type 'analyse_run -h' for valid options"

