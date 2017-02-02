# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm

from kronos.core.plot_handler import PlotHandler
from kronos.core.postprocess.run_data import RunData
from kronos.core.kronos_tools.print_colour import print_colour
from kronos.core.workload_data import WorkloadData
from kronos.core import time_signal


class RunPlotter(object):

    def __init__(self, run_dir):

        assert isinstance(run_dir, str)
        self.plot_handler = PlotHandler()
        self.run_dir = run_dir
        self.run_data = RunData(run_dir)

    def plot_run(self):
        """
        Loop over the run folders and plot run results..
        :return:
        """

        n_iterations = self.run_data.get_n_iterations()

        # Metrics requested at iteration 0
        ksf_data_0 = self.run_data.get_ksf_data(iteration=0)
        # ksf_scaled_sums_0 = ksf_data_0.scaled_sums
        ksf_unscaled_sums_0 = ksf_data_0.unscaled_sums

        # loop over all the iterations
        for i_iter in range(n_iterations):

            # ------- retrieve kpf data from iteration ----------
            kpf_data = self.run_data.get_kpf_data(iteration=i_iter)
            kpf_workload = kpf_data[0]
            tot_n_apps = len(kpf_workload.jobs)

            labels_list = set(j.label for j in kpf_workload.jobs)
            print 'labels_list', labels_list
            labels_jobs_dict = {}
            for lab in labels_list:
                for j in kpf_workload.jobs:
                    if j.label == lab:
                        if labels_jobs_dict.get(lab,None):
                            labels_jobs_dict[lab].append(j)
                        else:
                            labels_jobs_dict[lab] = [j]

            # print self.run_data.get_kpf_data(iteration=0)[0].total_metrics_sum_dict()

            # there should be only one workload in the kpf file
            try:
                assert len(kpf_data) == 1
            except AssertionError:
                print_colour("red", "kpf file from run profiling contains more than 1 workload")
            # ----------------------------------------------------

            # ------ retrieve data from ksp from iterations ------
            ksf_data = self.run_data.get_ksf_data(iteration=i_iter)
            # ----------------------------------------------------

            # --------------- global properties of the whole workload --------------------
            total_exe_time = max(sa.time_start+sa.duration for sa in kpf_workload.jobs) - \
                             min(sa.time_start for sa in kpf_workload.jobs)

            global_t0 = kpf_workload.min_time_start
            # ----------------------------------------------------------------------------

            # queuing times of jobs..
            queuing_times_vec = [sa['start_delay'] for sa in ksf_data.synth_app_json_data]
            queuing_times_vec.sort()

            # colors for plots
            color = iter(cm.rainbow(np.linspace(0, 1, len(labels_list))))

            # ---------- make plot ----------
            n_plots = len(kpf_workload.total_metrics_timesignals.keys()) + 3
            fig_size = (16, 3 * n_plots)
            plt.figure(self.plot_handler.get_fig_handle_ID(), figsize=fig_size)
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

                print 'start_time_vec: {}'.format(start_time_vec)
                print 'n_running_vec: {}'.format(n_running_vec)
                print 'nproc_running_vec: {}'.format(nproc_running_vec)
                print "all_vec_sort: {}".format(all_vec_sort)

                # initial plot with the running jobs/processes
                id_plot = 1
                plt.subplot(n_plots, 1, id_plot)
                plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
                plt.plot(queuing_times_vec + [total_exe_time],
                         range(len(queuing_times_vec)) + [len(queuing_times_vec) - 1],
                         color=line_color,
                         label=label)
                plt.ylabel('Cumulative queued jobs')
                plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
                plt.title('Time profiles, Number of apps: {} - (FL iteration: {})'.format(tot_n_apps, i_iter))

                id_plot += 1
                plt.subplot(n_plots, 1, id_plot)
                plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
                plt.plot(all_vec_sort[:, 0] - all_vec_sort[0, 0],
                         n_running_vec,
                         color=line_color,
                         label=label)
                plt.ylabel('running jobs')

                id_plot += 1
                plt.subplot(n_plots, 1, id_plot)
                plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
                plt.plot(all_vec_sort[:, 0] - all_vec_sort[0, 0],
                         nproc_running_vec,
                         color=line_color,
                         label=label)
                plt.ylabel('running processors')

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

                    if tt == len(total_metrics)-1:
                        plt.xlabel('time')

            plt.savefig(os.path.join(self.run_dir, 'time_profiles_iter-{}.png'.format(i_iter)))
            plt.close()

            # ------------ plot of requested and run metrics --------------------
            n_plots = 2
            fig_size = (16, 3*n_plots)
            plt.figure(self.plot_handler.get_fig_handle_ID(), figsize=fig_size)
            width = 0.8
            id_plot = 0

            # requested and run metrics..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(time_signal.time_signal_names))
            plt.bar(xx,
                    np.asarray([ksf_unscaled_sums_0[k] for k in time_signal.time_signal_names]),
                    width / 2.0,
                    color='red',
                    label='Requested metrics')
            plt.bar(xx + width / 2.0,
                    np.asarray([kpf_workload.total_metrics_sum_dict[k] for k in time_signal.time_signal_names]),
                    width / 2.0,
                    color='blue',
                    label='Run metrics')
            # plt.xlabel('metrics')
            # plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt.ylim(ymin=1)
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k in time_signal.time_signal_names])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            plt.title('Total metrics of iteration: {}'.format(i_iter))

            # requested and run metrics diff..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(time_signal.time_signal_names))
            plt.bar(xx + width / 4.0,
                    np.abs(np.asarray([ksf_unscaled_sums_0[k] for k in time_signal.time_signal_names]) -
                           np.asarray([kpf_workload.total_metrics_sum_dict[k] for k in time_signal.time_signal_names])) /
                    np.asarray([ksf_unscaled_sums_0[k] for k in time_signal.time_signal_names]),
                    width / 2.0,
                    color='green',
                    label='|req-run|/req_max')
            # plt.xlabel('metrics')
            # plt.yscale('log')
            # plt.ylim(ymin=1)
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k in time_signal.time_signal_names])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            plt.savefig(os.path.join(self.run_dir, 'summary_metrics_iter-{}.png'.format(i_iter)))
            plt.close()
