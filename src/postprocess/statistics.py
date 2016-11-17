import collections
import csv
import os
import json
import glob
import matplotlib.pyplot as plt
import numpy as np
import pickle


from logreader.scheduler_reader import PBSDataSet, AccountingDataSet
from config.config import Config
from kronos_tools.print_colour import print_colour
from time_signal import signal_types
import logreader
from plot_handler import PlotHandler


class Statistics(object):

    """
    This class implements statistics on the ingested data and synthetic apps data
    """

    def __init__(self, config=None):

        assert isinstance(config, Config)
        self.config = config

        self.summary_data = {}
        self.sa_data_list = []
        self.metrics_from_sa_iter0 = {el: 0 for el in signal_types}
        self.requested_metrics = None
        self.synthetic_workload = None
        self.plot_handler = PlotHandler()

    @staticmethod
    def dataset_statistics(dataset):

        if isinstance(dataset, AccountingDataSet) or isinstance(dataset, PBSDataSet):

            dataset_data = {}
            queue_list = list(set(i_job.queue_type for i_job in dataset.joblist))

            for iq in queue_list:

                list_jobs_in_queue = [i_job for i_job in dataset.joblist if i_job.queue_type == iq]

                n_jobs = len(list_jobs_in_queue)
                mean_runtime = sum([i_job.runtime for i_job in list_jobs_in_queue])/float(n_jobs)
                mean_queue_time = sum([i_job.time_in_queue for i_job in list_jobs_in_queue])/float(n_jobs)
                total_cpu_hours = sum([i_job.ncpus * i_job.runtime / 3600. for i_job in list_jobs_in_queue])

                if n_jobs >= 1:
                    dataset_data[iq] = collections.OrderedDict([('N jobs', '{:d}'.format(n_jobs)),
                                                                    ('mean runtime [s]', '{:.1f}'.format(mean_runtime)),
                                                                    ('mean queue time [s]', '{:.1f}'.format(mean_queue_time)),
                                                                    ('total cpu-hours', '{:.1f}'.format(total_cpu_hours))
                                                                    ])

        else:

            print_colour("orange", "plotter for {} dataset not implemented!".format(type(dataset)))
            dataset_data = {}

        return dataset_data

    @staticmethod
    def read_sa_metrics_from_jsons(dir_sa_jsons):
        """
        Read the synthetic apps in folder
        Note this routine should work as a "standalone" from the data read from the synthetic apps json files
        :return:
        """

        # read json files of the synthetic apps..
        json_files = glob.glob(os.path.join(os.path.realpath(dir_sa_jsons), "*.json"))
        if not json_files:
            raise OSError("no json files of synthetic apps found in {}".format(dir_sa_jsons))
        json_files.sort()

        # read json files and construct a dictionary of sa structures..
        sa_data_list = []
        for file_name in json_files:

            # Initialize the sa data
            sa_data = {}

            # Initialize frames
            sa_data['frames'] = {}
            for ss in signal_types.keys():
                sa_data['frames'][ss] = []

            # Read sa data from json file
            with open(file_name) as data_file:
                json_data = json.load(data_file)

            # read frame data and append to lists
            for ker_list in json_data['frames']:
                for ker in ker_list:
                    for k in ker.keys():
                        if k in signal_types:
                            sa_data['frames'][k].append(ker[k])

            # put zeros instead of empty frames
            for signal_name in sa_data['frames'].keys():
                if not sa_data['frames'][signal_name]:
                    sa_data['frames'][signal_name] = [0]

            # read the other keys..
            sa_data['num_procs'] = json_data['num_procs']
            sa_data['start_delay'] = json_data['start_delay']
            sa_data_list.append(sa_data)

        return sa_data_list

    def print_sa_stats(self):
        """
        Just print sums of synthetic apps
        """
        if not self.sa_data_list:
            raise ValueError("Synthetic apps jsons not processed.. ")
        else:
            print "---------------------------------------------------------"
            print "Total number of synthetic apps = {}".format(len(self.sa_data_list))
            print "Sums REQUESTED metrics (read from SApps):"
            for ss in signal_types.keys():
                print "    {} = {}".format(ss, self.metrics_from_sa_iter0[ss])

    def plot_sa_stats(self):
        """
        Plot statistics of the synthetic apps
        """

        synthetic_workload = self.read_workload_file()
        total_metrics = synthetic_workload.total_metrics_dict()

        # ---------- make plot ----------
        fig_size = (16, 6)
        plt.figure(self.plot_handler.get_fig_handle_ID(), figsize=fig_size)
        width = 0.8
        id_plot = 0
        n_plots = 2

        id_plot += 1
        plt.subplot(n_plots, 1, id_plot)
        plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
        xx = np.arange(len(total_metrics.keys()))
        plt.bar(xx, np.asarray([total_metrics[k] for k in signal_types.keys()]),
                width / 2.0,
                color='grey',
                label='Model WL')
        plt.bar(xx + width / 2.0, np.asarray([self.metrics_from_sa_iter0[k] for k in signal_types.keys()]),
                width / 2.0,
                color='red',
                label='Synth WL')
        plt.xlabel('metrics')
        plt.ylabel('sums over all jobs')
        plt.yscale('log')
        plt.ylim(ymin=1)
        plt_hdl = plt.gca()
        plt_hdl.set_xticks([i + width / 2. for i in xx])
        plt_hdl.set_xticklabels([k for k, v in self.metrics_from_sa_iter0.items()])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        id_plot += 1
        plt.subplot(n_plots, 1, id_plot)
        plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
        xx = np.arange(len(total_metrics.keys()))
        plt.bar(xx, np.asarray([total_metrics[k] for k in signal_types.keys()]),
                width / 2.0,
                color='grey',
                label='Model WL')
        plt.bar(xx + width / 2.0, np.asarray([self.metrics_from_sa_iter0[k] / self.config.plugin['tuning_factors'][k] for k in signal_types.keys()]),
                width / 2.0,
                color='red',
                label='Synth WL (rescaled)')
        plt.xlabel('metrics')
        plt.ylabel('sums over all jobs')
        plt.yscale('log')
        plt.ylim(ymin=1)
        plt_hdl = plt.gca()
        plt_hdl.set_xticks([i + width / 2. for i in xx])
        plt_hdl.set_xticklabels([k for k in signal_types.keys()])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        plt.savefig(os.path.join(self.config.dir_output, 'out_sa_plots.png'))
        plt.show()

    def calculate_run_metrics(self):
        """
        Read json files of the run jsons
        :return:
        """

        # read initial requested metrics from folder "iteration-0"
        iter0_dir = os.path.join(self.config.post_process["dir_sa_run_output"], 'iteration-0/sa_jsons')
        self.sa_data_list = self.read_sa_metrics_from_jsons(iter0_dir)
        for sa in self.sa_data_list:
            for tt in sa['frames'].keys():
                self.metrics_from_sa_iter0[tt] += sum(sa['frames'][tt])

        print "total SUMS from SA_ITERATION 0: "
        for ss in signal_types.keys():
            print "    {} = {}".format(ss, self.metrics_from_sa_iter0[ss])

        if not self.config.post_process["fb_loop"]:
            # plot a simple run (signals sums)
            iteration_dirs = [self.config.post_process["dir_sa_run_output"]]
            dir_plot_output = self.config.post_process["dir_sa_run_output"]
            self.plot_run_sums(self.metrics_from_sa_iter0, iteration_dirs, dir_plot_output)

        else:  # plot a feedback loop run (signals sums + run quantities)
            iteration_dirs = list()
            for root, dirs, files in os.walk(self.config.post_process["dir_sa_run_output"], topdown=False):
                for name_iter_dir in dirs:
                    if "iteration-" in name_iter_dir:
                        iteration_dir_run = os.path.join(name_iter_dir, 'run_jsons')
                        iteration_dirs.append(os.path.join(root, iteration_dir_run))

            # sort iteration dirs
            iteration_dirs.sort()

            # create output plot folder inside the run folder:
            dir_plot_output = os.path.join(self.config.post_process["dir_sa_run_output"], 'run_postprocess')
            if not os.path.exists(dir_plot_output):
                os.makedirs(dir_plot_output)

            # make plots of total quantities
            self.plot_run_sums(self.metrics_from_sa_iter0, iteration_dirs, dir_plot_output)

            # plots of tuning factors from log file
            self.plot_from_logfile(dir_plot_output)

    def plot_run_sums(self, requested_metrics, iteration_dirs, dir_plot_output):
        """
        Loop over the run folders and plot run results..
        :param requested_metrics:
        :param iteration_dirs:
        :return:
        """

        # loop over all the iterations
        for iter, iteration_dir in enumerate(iteration_dirs):
            allinea_jsons_sums, sa_out_dataset = self.read_allinea_run_jsons(iteration_dir)

            total_exe_time = max(sa.time_end for sa in sa_out_dataset.joblist) - \
                             min(sa.time_start for sa in sa_out_dataset.joblist)

            self.print_allinea_json_sums(allinea_jsons_sums, total_exe_time)

            # queuing times of jobs..
            queuing_times_vec = [sa['start_delay'] for sa in self.sa_data_list]
            queuing_times_vec.sort()

            # calculate running jobs in time..
            start_time_vec = np.asarray([sa.time_start for sa in sa_out_dataset.joblist])
            start_time_vec = start_time_vec.reshape(start_time_vec.shape[0], 1)
            plus_vec = np.hstack((start_time_vec, np.ones(start_time_vec.shape)))
            end_time_vec = np.asarray([sa.time_end for sa in sa_out_dataset.joblist])
            end_time_vec = end_time_vec.reshape(end_time_vec.shape[0], 1)
            minus_vec = np.hstack((end_time_vec, -1 * np.ones(end_time_vec.shape)))
            all_vec = np.vstack((plus_vec, minus_vec))
            all_vec_sort = all_vec[all_vec[:, 0].argsort()]
            n_running_vec = np.cumsum(all_vec_sort[:, 1])

            # calculate used procs in time..
            proc_time_vec = np.asarray([sa.ncpus for sa in sa_out_dataset.joblist])
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
            plt.title("plot iteration {}".format(iter))

            # requested and run metrics..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(requested_metrics.keys()))
            plt.bar(xx,
                    np.asarray([self.metrics_from_sa_iter0[k] for k in signal_types.keys()]),
                    width / 2.0,
                    color='red',
                    label='Requested metrics')
            plt.bar(xx + width / 2.0,
                    np.asarray([allinea_jsons_sums[k] for k in signal_types.keys()]),
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
            xx = np.arange(len(requested_metrics.keys()))
            plt.bar(xx + width / 4.0,
                    np.abs(np.asarray([self.metrics_from_sa_iter0[k] for k in signal_types.keys()]) -
                           np.asarray([allinea_jsons_sums[k] for k in signal_types.keys()])) /
                    np.asarray([self.metrics_from_sa_iter0[k] for k in signal_types.keys()]),
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
            plt.savefig(os.path.join(dir_plot_output, 'out_run_plots_iter-{}.png'.format(iter)))
            plt.close()
            # plt.show()


    def read_workload_file(self):
        """
        Read pickle file of the synthetic workload
        :return:
        """
        # load up the workload file ad calculated the requested sums..
        workload_file = os.path.join(self.config.dir_output, 'sa_workload_pickle')
        with open(workload_file, 'r') as f:
            synthetic_workload = pickle.load(f)

        return synthetic_workload



    def read_allinea_run_jsons(self, allinea_path=None):
        """
        Read Allinea json files from the output folder..
        :return:
        """
        # load up run output..
        if allinea_path:
            sa_out_dataset = logreader.ingest_data("allinea", allinea_path)
        else:
            sa_out_dataset = logreader.ingest_data("allinea", self.config.post_process['dir_sa_run_output'])

        allinea_jsons_sums = {el: 0 for el in signal_types}
        for sa in sa_out_dataset.joblist:
            for tt in sa.timesignals.keys():
                allinea_jsons_sums[tt] += sum(sa.timesignals[tt].yvalues)

        return allinea_jsons_sums, sa_out_dataset

    def print_allinea_json_sums(self, allinea_sums, tot_time):
        """
        print the metric sums as they are read from the alliena jsons
        :param allinea_sums: allinea metrics sums
        :param tot_time: total run-time of the workload
        :return: None
        """
        print "---------------------------------------------------------"
        print "Sums RUN quantities:"
        print "Total execution time: {}".format(tot_time)
        for ss in signal_types.keys():
            print "    {} = {}".format(ss, allinea_sums[ss])

    def plot_from_logfile(self, dir_plot_output):

        # log file in output folder
        log_file = os.path.join(self.config.post_process["dir_sa_run_output"], 'log_file.txt')
        reader = csv.reader(open(log_file), delimiter=" ")
        reader_lines = [[n for n in ll if n is not ''] for ll in reader]
        header = reader_lines[0]
        ref_values = reader_lines[1]
        iterations = reader_lines[2:]

        # plot signal convergence
        fig_size = (12, 14)
        plt.figure(self.plot_handler.get_fig_handle_ID(), figsize=fig_size)
        id_plot = 0
        n_plots = len(signal_types.keys())
        iter_vec = range(0, len(iterations))

        for tt, ts in enumerate(header):

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)

            ref_vec = [float(ref_values[tt]) for ii in iter_vec]
            val_vec = [float(iterations[ii][tt]) for ii in iter_vec]

            plt.plot(iter_vec, ref_vec, color='b', label='reference signal')
            plt.plot(iter_vec, val_vec, color='r', label='measured signal')

            plt.xlabel('iterations')
            plt.xticks(iter_vec)
            plt.ylabel(ts)
            plt.ylim(ymin=0)
            plt.ylim(ymax=1.2*max([max(ref_vec), max(val_vec)]))

            if tt == 0:
                plt.title("Signals convergence")
                # plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
                plt.legend()

        plt.savefig(os.path.join(dir_plot_output, 'convergence.png'.format(iter)))
        plt.show()

