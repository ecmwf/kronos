
import numpy as np
import matplotlib.pyplot as plt

from plot_handler import PlotHandler
from postprocess.run_data import RunData
from time_signal import signal_types
from kronos_tools.print_colour import print_colour


class RunPlotter(object):

    def __init__(self, run_path):

        assert isinstance(run_path, unicode)
        self.plot_handler = PlotHandler()

        self.run_data = RunData(run_path)

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

        # print "ksf_unscaled_sums_0", ksf_unscaled_sums_0

        # loop over all the iterations
        for i_iter in range(n_iterations):

            # ------- retrieve kpf data from iteration ----------
            kpf_data = self.run_data.get_kpf_data(iteration=i_iter)
            kpf_workload = kpf_data[0]

            # print self.run_data.get_kpf_data(iteration=0)[0].total_metrics_sum_dict()

            # there should be only one workload in the kpf file
            try:
                assert len(kpf_data) == 1
            except AssertionError:
                print_colour("red", "kpf file from ru nprofiling contains more than 1 workload")
            # ----------------------------------------------------

            # ------ retrieve data from ksp from iterations ------
            ksf_data = self.run_data.get_ksf_data(iteration=i_iter)
            # ----------------------------------------------------

            total_exe_time = max(sa.time_start+sa.duration for sa in kpf_workload.jobs) - \
                             min(sa.time_start for sa in kpf_workload.jobs)

            # queuing times of jobs..
            queuing_times_vec = [sa['start_delay'] for sa in ksf_data.synth_app_json_data]
            queuing_times_vec.sort()

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

            # ---------- make plot ----------
            fig_size = (16, 9)
            plt.figure(self.plot_handler.get_fig_handle_ID(), figsize=fig_size)
            width = 0.8
            id_plot = 0
            n_plots = 3

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            plt.plot(queuing_times_vec + [total_exe_time],
                     range(len(queuing_times_vec)) + [len(queuing_times_vec) - 1],
                     color='r',
                     label='queued jobs')
            plt.plot(all_vec_sort[:, 0] - all_vec_sort[0, 0],
                     n_running_vec,
                     color='b',
                     label='running jobs')
            plt.plot(all_vec_sort[:, 0] - all_vec_sort[0, 0],
                     nproc_running_vec,
                     color='m',
                     label='running processors')
            plt.xlabel('time')
            plt.ylabel('jobs')
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            plt.title("plot iteration {}".format(i_iter))

            # requested and run metrics..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(signal_types.keys()))
            plt.bar(xx,
                    np.asarray([ksf_unscaled_sums_0[k] for k in signal_types.keys()]),
                    width / 2.0,
                    color='red',
                    label='Requested metrics')
            plt.bar(xx + width / 2.0,
                    np.asarray([kpf_workload.total_metrics_sum_dict()[k] for k in signal_types.keys()]),
                    width / 2.0,
                    color='blue',
                    label='Run metrics')
            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt.ylim(ymin=1)
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k in signal_types.keys()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

            # requested and run metrics diff..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(signal_types.keys()))
            plt.bar(xx + width / 4.0,
                    np.abs(np.asarray([ksf_unscaled_sums_0[k] for k in signal_types.keys()]) -
                           np.asarray([kpf_workload.total_metrics_sum_dict()[k] for k in signal_types.keys()])) /
                    np.asarray([ksf_unscaled_sums_0[k] for k in signal_types.keys()]),
                    width / 2.0,
                    color='green',
                    label='|req-run|/req_max')
            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            # plt.yscale('log')
            # plt.ylim(ymin=1)
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k in signal_types.keys()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            # plt.savefig(os.path.join(dir_plot_output, 'out_run_plots_iter-{}.png'.format(i_iter)))
            # plt.close()
            plt.show()
