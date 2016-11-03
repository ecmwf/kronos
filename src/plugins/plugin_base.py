import os
import glob
import json
import pickle
import matplotlib.pyplot as plt
import numpy as np

from time_signal import signal_types
# from kronos_tools.print_colour import print_colour
import logreader


class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config

    def ingest_data(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def postprocess(self):
        """
        Do some post-processing of the synthetic apps (modelled and run)
        :return:
        """

        config_pp = self.config.post_process

        # ---------------------- read sa json's (run input) ------------------------
        # list of json files in folder..
        json_files = glob.glob(os.path.join(os.path.realpath(config_pp['dir_sa_run_input']), "*.json"))
        if not json_files:
            raise OSError("no json files of synthetic apps found in {}".format(config_pp['dir_sa_run_input']))

        json_files.sort()
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

        # ------------- group queuing times.. -------------
        qqt = [sa['start_delay'] for sa in sa_data_list]
        qqt.sort()

        print "---------------------------------------------------------"
        print "Total number of synthetic apps = {}".format(len(sa_data_list))
        print "Sums REQUESTED quantities:"
        sums_sig = {el: 0 for el in signal_types}
        for sa in sa_data_list:
            for tt in sa['frames'].keys():
                sums_sig[tt] += sum(sa['frames'][tt])

        for ss in sums_sig.keys():
            print "    {} = {}".format(ss, sums_sig[ss])

        # --------------- check output if requested -----------------
        if (config_pp["post_process_scope"] == "output") or (config_pp["post_process_scope"] == "both"):
            sa_out_dataset = logreader.ingest_data("allinea", config_pp['dir_sa_run_output'])
            allinea_jsons_sums = {el: 0 for el in signal_types}
            for sa in sa_out_dataset.joblist:
                for tt in sa.timesignals.keys():
                    allinea_jsons_sums[tt] += sum(sa.timesignals[tt].yvalues)

            total_exe_time = max(sa.time_end for sa in sa_out_dataset.joblist) - min(
                sa.time_start for sa in sa_out_dataset.joblist)

            # calculate submitted and running jobs in time..
            start_time_vec = np.asarray([sa.time_start for sa in sa_out_dataset.joblist])
            start_time_vec = start_time_vec.reshape(start_time_vec.shape[0], 1)
            plus_vec = np.hstack((start_time_vec, np.ones(start_time_vec.shape)))

            end_time_vec = np.asarray([sa.time_end for sa in sa_out_dataset.joblist])
            end_time_vec = end_time_vec.reshape(end_time_vec.shape[0], 1)
            minus_vec = np.hstack((end_time_vec, -1 * np.ones(end_time_vec.shape)))

            all_vec = np.vstack((plus_vec, minus_vec))
            all_vec_sort = all_vec[all_vec[:, 0].argsort()]
            n_running_vec = np.cumsum(all_vec_sort[:, 1])

            print "---------------------------------------------------------"
            print "Sums RUN quantities:"
            print "Total execution time: {}".format(total_exe_time)
            for ss in allinea_jsons_sums.keys():
                print "    {} = {}".format(ss, allinea_jsons_sums[ss])

        # ========================= summary plots ================================
        with open(os.path.join(self.config.dir_output, 'sa_workload_pickle'), 'r') as f:
            synth_wl = pickle.load(f)
        total_metrics = synth_wl.total_metrics_dict()

        # ---------- make plot ----------
        fig_size = (16, 8)
        plt.figure(101, figsize=fig_size)
        width = 0.8
        id_plot = 0

        if config_pp["post_process_scope"] == "input":
            n_plots = 2
        elif config_pp["post_process_scope"] == "output":
            n_plots = 3
        elif config_pp["post_process_scope"] == "both":
            n_plots = 5

        if (config_pp["post_process_scope"] == "input") or (config_pp["post_process_scope"] == "both"):

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(total_metrics.keys()))
            plt.bar(xx, np.asarray([v for k, v in total_metrics.items()]),
                    width/2.0,
                    color='grey',
                    label='unscaled')
            plt.bar(xx+width/2.0, np.asarray([v for k, v in sums_sig.items()]),
                    width/2.0,
                    color='red',
                    label='synthetic apps')
            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i+width/2. for i in xx])
            plt_hdl.set_xticklabels([k for k, v in sums_sig.items()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(total_metrics.keys()))
            plt.bar(xx, np.asarray([v for k, v in total_metrics.items()]),
                    width/2.0,
                    color='grey',
                    label='unscaled')
            plt.bar(xx+width/2.0, np.asarray([v/self.config.plugin['tuning_factors'][k] for k, v in sums_sig.items()]),
                    width/2.0,
                    color='red',
                    label='synthetic apps')
            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i+width/2. for i in xx])
            plt_hdl.set_xticklabels([k for k, v in sums_sig.items()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        if (config_pp["post_process_scope"] == "output") or (config_pp["post_process_scope"] == "both"):

            # -------------------- queued and running jobs.. ------------------------
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(total_metrics.keys()))
            plt.plot(all_vec_sort[:, 0] - all_vec_sort[0, 0], n_running_vec, color='b', label='running jobs')
            plt.plot(qqt + [total_exe_time], range(len(qqt)) + [len(qqt) - 1], color='r', label='queued jobs')
            plt.xlabel('time')
            plt.ylabel('jobs')
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

            # requested and run metrics..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(total_metrics.keys()))
            plt.bar(xx, np.asarray([v for k, v in sums_sig.items()]), width/2.0, color='red',
                    label='input synthetic apps')
            plt.bar(xx+width/2.0, np.asarray([v for k, v in allinea_jsons_sums.items()]), width/2.0, color='blue',
                    label='run synthetic apps')
            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k, v in sums_sig.items()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

            # requested and run metrics diff..
            id_plot += 1
            plt.subplot(n_plots, 1, id_plot)
            plt.subplots_adjust(left=0.2, right=0.8, top=0.9, bottom=0.1)
            xx = np.arange(len(total_metrics.keys()))
            plt.bar(xx,
                    np.asarray([v for k, v in sums_sig.items()]) - np.asarray([v for k, v in allinea_jsons_sums.items()]),
                    width/2.0,
                    color='green',
                    label='diff req-run')

            plt.xlabel('metrics')
            plt.ylabel('sums over all jobs')
            plt.yscale('log')
            plt_hdl = plt.gca()
            plt_hdl.set_xticks([i + width / 2. for i in xx])
            plt_hdl.set_xticklabels([k for k, v in sums_sig.items()])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        # plt.savefig(out_dir + '/' + "hist_pg16.png")
        # plt.close(101)
        plt.show()
        # ===============================================================





