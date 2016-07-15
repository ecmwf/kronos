
import numpy as np
import matplotlib.pyplot as plt

# from sklearn.decomposition import PCA
from plot_handler import PlotHandler
from time_signal import TimeSignal
from synthetic_app import SyntheticApp


import clustering


from tools import mytools


class IOWSModel(object):

    """
    A model of the workload that applies:

      1) clustering of the apps
      2) visualization
      3) scaling factor and redeployment of synthetic apps
    """

    def __init__(self, config_options):

        self.input_workload = None
        self.synth_apps = []
        self.schedule = []

        self.out_dir = config_options.DIR_OUTPUT

        self.total_metrics_nbins = config_options.IOWSMODEL_TOTAL_METRICS_NBINS
        self.kmeans_maxiter = config_options.IOWSMODEL_KMEANS_MAXITER
        self.kmeans_rseed = config_options.IOWSMODEL_KMEANS_KMEANS_RSEED
        self.job_impact_index_rel_tsh = config_options.IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH
        self.jobs_n_bins = config_options.WORKLOADCORRECTOR_JOBS_NBINS
        self.supported_synth_apps = config_options.IOWSMODEL_SUPPORTED_SYNTH_APPS

        self.cluster_centers = {}
        self.cluster_labels = {}
        # self.pca = None
        # self.pca_2d = None

        self.aggregated_metrics = {}
        self._n_clusters = None
        self.syntapp_list = []

        self.clustering_key = None

        # num of kernels for each synth app, temporarily set as constant (TODO: to change..)
        self.n_kernels = config_options.WORKLOADCORRECTOR_JOBS_NBINS
        self.n_time_signals = len(config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES)
        self.ts_names = [i_ts[0] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]
        self.job_signal_dgt = [i_ts[3] for i_ts in config_options.WORKLOADCORRECTOR_LIST_TIME_NAMES]

    def set_input_workload(self, input_workload):
        self.input_workload = input_workload

    def create_scaled_workload(self, clustering_key, which_clust, n_clust_pc):
        """ function that creates the scaled workload """

        # call the clustering first..
        self.apply_clustering(clustering_key, which_clust, n_clust_pc)
        self.calculate_total_metrics()

        scaling_factor = float(n_clust_pc)/100.

        print "scaling factor: ", scaling_factor

        # # -------- create pareto front with points of the WL ----------
        # wl_time_points = []
        # for (i_bin, val) in enumerate(self.input_workload.total_metrics[0].yvalues_bins):
        #     param_in_bin = [iTS.yvalues_bins[i_bin] for iTS in self.input_workload.total_metrics]
        #     wl_time_points.append(param_in_bin)
        #
        # (pareto_points, pareto_idxes, dominated_points) = mytools.simple_cull(wl_time_points, mytools.dominates)
        #
        # # create time signals from PF points
        # pf_ts_list = []
        # for iTS in self.input_workload.total_metrics:
        #     ts_name = iTS.name
        #     ts_signal_x = iTS.xvalues_bins[pareto_idxes]
        #     ts_signal_y = iTS.yvalues_bins[pareto_idxes]
        #     ts = TimeSignal()
        #     ts.create_ts_from_values(ts_name, ts_signal_x, ts_signal_y)
        #     pf_ts_list.append(ts)
        #
        # # append a synthetic job with PF time series to synthetic app list
        # app = SyntheticApp()
        # app.job_name = "JOB-pareto"
        # app.fill_time_series(pf_ts_list)
        # self.syntapp_list.append(app)

        # ---------- append jobs from clusters to synthetic app list ------------
        n_bins = len(self.input_workload.LogData[0].timesignals[0].xvalues_bins)

        for iC in range(0, self._n_clusters):
            pf_ts_list = []
            for tt in range(0, self.n_time_signals):
                idx_ts = np.arange(0, n_bins) + n_bins * tt
                retrieved_yvals = self.cluster_centers[iC, idx_ts]

                # create ts from retrieved_yvals
                ts_name = self.input_workload.LogData[0].timesignals[tt].name
                ts_type = self.input_workload.LogData[0].timesignals[tt].ts_type
                ts_group = self.input_workload.LogData[0].timesignals[tt].ts_group
                ts_signal_x = np.arange(0, len(retrieved_yvals))*1.0  # just a dummy value (not actually needed..)
                ts_signal_y = retrieved_yvals
                ts = TimeSignal()
                ts.create_ts_from_values(ts_name, ts_type, ts_group, ts_signal_x, ts_signal_y)

                # we cannot re-digitize the clusters as xvalues are not meaningful..
                # ts.digitize(10, 'sum')
                pf_ts_list.append(ts)

            # create app from cluster time signals
            app = SyntheticApp()
            app.job_name = "JOB-cluster-" + str(iC)
            app.fill_time_series(pf_ts_list)
            self.syntapp_list.append(app)

        # # ------ finally append non-clustered jobs ----------
        # for (jj, i_job) in enumerate(self.input_workload.LogData):
        #     if i_job.job_impact_index_rel >= self.job_impact_index_rel_tsh:
        #         app = SyntheticApp()
        #         app.job_name = "JOB-NON-clustered-" + str(jj)
        #         app.fill_time_series(i_job.timesignals)
        #         self.syntapp_list.append(app)

        # ------ fill in the synthetic apps metadata --------
        maxStartTime_fromT0 = self.input_workload.maxStartTime_fromT0
        for (ii, i_app) in enumerate(self.syntapp_list):
            i_app.jobID = 0
            i_app.job_name = "appID-"+str(ii)
            # set start time to a random number (TODO: change this to a more sensible criterion)
            i_app.time_start = np.random.random() * maxStartTime_fromT0 * scaling_factor

        # Calculate sums of total metrics signals (of the original WL)
        tot_metrics_sums = np.ndarray(self.n_time_signals)
        for (ii, i_sign) in enumerate(self.input_workload.total_metrics):
            tot_metrics_sums[ii] = sum(i_sign.yvalues)

        # Calculate sums of synthetic (needed for rescaling each ts to match target scaling factor)
        clust_metrics_sums = np.ndarray(self.n_time_signals)
        for (ii, i_ts) in enumerate(self.aggregated_metrics):
            clust_metrics_sums[ii] = sum(i_ts.yvalues)

        # Now actually apply the scaling factor to all the ts of each synthetic app
        for (ss, i_app) in enumerate(self.syntapp_list):
            for (tt, i_ts) in enumerate(i_app.time_signals):
                if clust_metrics_sums[tt]:
                    i_ts.yvalues = i_ts.yvalues / (clust_metrics_sums[tt]) * tot_metrics_sums[tt] * scaling_factor
                else:
                    i_ts.yvalues *= 0.0

    def apply_clustering(self, clustering_key, which_clust, n_clust_pc=-1):

        self.clustering_key = clustering_key
        self._n_clusters = max(1, int(n_clust_pc / 100. * len(self.input_workload.LogData)))

        # do the clustering only if scaling factor is less than 100
        if n_clust_pc < 100.:

            if clustering_key == "spectral":
                pass

                # # clustering
                # start = time.clock()
                #
                # # NOTE: n_signals assumes that all the jobs signals have the same num of signals
                # # NOTE: n_bins assumes that all the time signals have the same num of points
                # n_signals = len(self.input_workload.LogData[0].timesignals)
                # n_freq = self.input_workload.LogData[0].timesignals[0].freqs.size
                # data = np.array([]).reshape(0, (n_freq * 3) * n_signals)
                # for i_job in self.input_workload.LogData:
                #     data_row = []
                #     for i_sign in i_job.timesignals:
                #         data_row = np.hstack((data_row, i_sign.freqs.tolist() + i_sign.ampls.tolist() + i_sign.phases.tolist()))
                #
                #     data = np.vstack((data, data_row))
                #
                # # clustering
                # cluster_method = clustering.factory(which_clust, data)
                # self._n_clusters = cluster_method.train_method(self._n_clusters, self.kmeans_maxiter)
                # self.cluster_centers = cluster_method.clusters
                # self.cluster_labels = cluster_method.labels
                #
                # # # calculate 2D PCA
                # # self.pca = PCA(n_components=2).fit(self.cluster_centers)
                # # self.pca_2d = self.pca.transform(self.cluster_centers)
                #
                # # rebuild the centroids signals
                # idx1 = np.arange(0, n_freq)
                # cluster_all_output = np.array([]).reshape(len(self.input_workload.LogData[0].timesignals) + 1, 0)
                #
                # for (cc, i_job) in enumerate(self.input_workload.LogData):
                #
                #     # load the time of this job signal..
                #     iC = self.cluster_labels[cc]
                #     job_times = i_job.timesignals[0].xvalues
                #     block_vec = np.array([]).reshape(0, len(job_times))
                #     block_vec = np.vstack((block_vec, job_times))
                #
                #     # retrieve the appropriate time signal from cluster set (only if the impact index is low..)
                #     if i_job.job_impact_index_rel <= self.job_impact_index_rel_tsh:
                #         for (tt, i_ts_clust) in enumerate(i_job.timesignals):
                #             idx_f = idx1 + n_freq * 0 + (n_freq * 3 * tt)
                #             idx_a = idx1 + n_freq * 1 + (n_freq * 3 * tt)
                #             idx_p = idx1 + n_freq * 2 + (n_freq * 3 * tt)
                #             retrieved_f = self.cluster_centers[iC, idx_f]
                #             retrieved_a = self.cluster_centers[iC, idx_a]
                #             retrieved_p = self.cluster_centers[iC, idx_p]
                #             yy=abs(mytools.freq_to_time(job_times - job_times[0], retrieved_f, retrieved_a, retrieved_p))
                #             block_vec = np.vstack((block_vec, yy))
                #     else:
                #         print "i_job:", cc, " is not clustered.."
                #         for (tt, i_ts_clust) in enumerate(i_job.timesignals):
                #             yy = i_ts_clust.yvalues
                #             block_vec = np.vstack((block_vec, yy))
                #
                #     cluster_all_output = np.hstack((cluster_all_output, block_vec))
                # cluster_all_output = cluster_all_output[:, cluster_all_output[0, :].argsort()]
                #
                # # calculate the total metrics
                # ts_names = [i_ts.name for i_ts in self.input_workload.LogData[0].timesignals]
                # list_signals = []
                #
                # for i_ts in range(0, len(ts_names)):
                #     t_sign = TimeSignal()
                #     t_sign.create_ts_from_values(ts_names[i_ts], cluster_all_output[0, :], cluster_all_output[i_ts+1, :])
                #     t_sign.digitize(self.total_metrics_nbins, 'sum')
                #     list_signals.append(t_sign)
                #
                # self.aggregated_metrics = list_signals
                #
                # print "elapsed time: ", time.clock() - start

            elif clustering_key == "time_plane":

                # NOTE: n_signals assumes that all the jobs signals have the same num of signals
                # NOTE: n_bins assumes that all the time signals have the same
                # num of points
                data = np.array([]).reshape(0, self.jobs_n_bins * self.n_time_signals)

                for i_job in self.input_workload.LogData:
                    data_row = [item for i_sign in i_job.timesignals for item in i_sign.yvalues_bins]
                    data = np.vstack((data, data_row))

                # clustering
                cluster_method = clustering.factory(which_clust, data)
                cluster_method.train_method(self._n_clusters, self.kmeans_maxiter)
                self.cluster_centers = cluster_method.clusters
                self.cluster_labels = cluster_method.labels

                # # calculate 2D PCA
                # self.pca = PCA(n_components=2).fit(self.cluster_centers)
                # self.pca_2d = self.pca.transform(self.cluster_centers)

        else:  # copy jobs directly into clusters list..

            # NOTE: n_signals assumes that all the jobs signals have the same num of signals
            # NOTE: n_bins assumes that all the time signals have the same
            # num of points
            data = np.array([]).reshape(0, self.jobs_n_bins * self.n_time_signals)
            for i_job in self.input_workload.LogData:
                data_row = [item for i_sign in i_job.timesignals for item in i_sign.yvalues_bins]
                data = np.vstack((data, data_row))

            self.cluster_centers = data
            self.cluster_labels = np.arange(0, len(self.input_workload.LogData))

    def calculate_total_metrics(self):
        """ Calculate global metrics for all the jobs """

        # rebuild the centroids signals
        cluster_all_output = np.array([]).reshape(len(self.input_workload.LogData[0].timesignals) + 1, 0)

        for (cc, i_job) in enumerate(self.input_workload.LogData):

            iC = self.cluster_labels[cc]
            block_vec = np.array([]).reshape(0, len(i_job.timesignals[0].xvalues_bins))
            block_vec = np.vstack((block_vec, i_job.timesignals[0].xvalues_bins))

            # retrieve the appropriate time signal from cluster set (only if the impact index is low..)
            # if i_job.job_impact_index_rel <= self.job_impact_index_rel_tsh:
            for (tt, i_ts_clust) in enumerate(i_job.timesignals):
                idx_ts = np.arange(0, self.jobs_n_bins) + self.jobs_n_bins * tt
                retrieved_yvals = self.cluster_centers[iC, idx_ts]
                block_vec = np.vstack((block_vec, retrieved_yvals))
            # else:
            #     print "i_job:", cc, " is not clustered.."
            #     for (tt, i_ts_clust) in enumerate(i_job.timesignals):
            #         retrieved_yvals = i_ts_clust.yvalues_bins
            #         block_vec = np.vstack((block_vec, retrieved_yvals))

            cluster_all_output =np.hstack((cluster_all_output, block_vec))
        cluster_all_output = cluster_all_output[:, cluster_all_output[0, :].argsort()]

        # calculate the total metrics
        ts_names = [i_ts.name for i_ts in self.input_workload.LogData[0].timesignals]
        ts_types = [i_ts.ts_type for i_ts in self.input_workload.LogData[0].timesignals]
        ts_groups = [i_ts.ts_group for i_ts in self.input_workload.LogData[0].timesignals]
        list_signals = []

        for tt, i_ts in enumerate(range(0, len(ts_names))):
            t_sign = TimeSignal()
            t_sign.create_ts_from_values(ts_names[i_ts], ts_types[i_ts], ts_groups[i_ts],
                                         cluster_all_output[0, :], cluster_all_output[i_ts + 1, :])
            t_sign.digitize(self.total_metrics_nbins, self.job_signal_dgt[tt])
            list_signals.append(t_sign)

        self.aggregated_metrics = list_signals

    # def export_scaled_workload(self):
    #
    #     # prepare JSON data
    #     json_all_synth_app = {}
    #     n_bins = 1
    #     for i_app in self.syntapp_list:
    #         job_entry = i_app.make_kernels_from_ts(n_bins, self.job_signal_dgt, self.supported_synth_apps)
    #         json_all_synth_app[i_app.job_name] = job_entry
    #
    #     self.plot_sanity_check(json_all_synth_app)
    #
    #     with open(self.out_dir+'/dummy.json', 'w') as f:
    #         json.encoder.FLOAT_REPR = lambda o: format(o, '.2f')
    #         json.dump(json_all_synth_app, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': '))

    def export_scaled_workload(self):

        n_bins = 1

        json_all_synth_app = {}

        for (idx_app, i_app) in enumerate(self.syntapp_list):
            i_app.write_sa_json(n_bins, self.job_signal_dgt, self.supported_synth_apps, self.out_dir, idx_app)
            job_entry = i_app.get_json_formatted_data()
            json_all_synth_app[i_app.job_name] = job_entry

        # Sanity check in/out
        # < NOTE: this only works for n_bin = 1 >
        self.plot_sanity_check(json_all_synth_app)

    def make_plots(self, plt_tag):
        """ Plotting routine """

        np.random.seed(1)
        color_vec = np.random.rand(3, len(self.input_workload.LogData[0].timesignals))

        # plot real vs clustered
        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 20), dpi=80, facecolor='w', edgecolor='k')
        plt.title('time series ' + 'cluster_NC=' + str(self._n_clusters))
        tot_signals = self.aggregated_metrics

        for (cc, i_ts) in enumerate(self.input_workload.LogData[0].timesignals):
            plt.subplot(len(tot_signals), 1, cc + 1)
            xx = self.input_workload.total_metrics[cc].xedge_bins[:-1]
            yy = self.input_workload.total_metrics[cc].yvalues_bins
            tt_dx = self.input_workload.total_metrics[cc].dx_bins
            plt.bar(xx, yy, tt_dx, color='b')
            plt.bar(tot_signals[cc].xedge_bins[:-1], tot_signals[cc].yvalues_bins, tot_signals[cc].dx_bins,
                    color=color_vec[:, cc])
            plt.legend(['raw data', 'clustered'], loc=2)
            if yy.any() and tot_signals[cc].yvalues_bins.any():
                plt.yscale('log')
            plt.ylabel(tot_signals[cc].name)
            plt.xlabel('time [s]')
        plt.savefig(self.out_dir + '/' + plt_tag + '_plot' + '_cluster_NC=' + str(self._n_clusters) + '.png')
        plt.close(i_fig)

        # plot real vs clustered ERROR
        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 18), dpi=80, facecolor='w', edgecolor='k')
        plt.title('time series ERROR' + 'cluster_NC=' + str(self._n_clusters))
        tot_signals = self.aggregated_metrics

        for (cc, i_ts) in enumerate(self.input_workload.LogData[0].timesignals):
            plt.subplot(len(tot_signals), 1, cc + 1)
            xx = self.input_workload.total_metrics[cc].xedge_bins[:-1]
            yy = self.input_workload.total_metrics[cc].yvalues_bins
            tt_dx = self.input_workload.total_metrics[cc].dx_bins
            err_vec = [abs((x - y) / max(y,1.e-6) * 100.) for x, y in zip(tot_signals[cc].yvalues_bins, yy)]
            plt.bar(tot_signals[cc].xedge_bins[:-1], [i + 1e-20 for i in err_vec], tot_signals[cc].dx_bins,
                    color=color_vec[:, cc])
            plt.legend(['raw data', 'clustered'], loc=2)
            plt.ylabel(tot_signals[cc].name)
            plt.xlabel('time [s]')
        plt.savefig(self.out_dir + '/' + plt_tag + '_plot' + '_cluster_NC=' + str(self._n_clusters) + '_ERROR.png')
        plt.close(i_fig)

        # # plot PCA centroids..
        # if self.pca_2d.shape[1] > 1:
        #     i_fig = PlotHandler.get_fig_handle_ID()
        #     plt.figure(i_fig)
        #     plt.title('cluster PC''s')
        #     plt.plot(self.pca_2d[:, 0], self.pca_2d[:, 1], 'bo')
        #     plt.savefig(self.out_dir + '/' + plt_tag + '_plot' + '_cluster_PCA=2_' + str(self._n_clusters) + '.png')
        #     plt.close(i_fig)

    def plot_sanity_check(self, sa_list):
        """ Over Plot ts input and synthetic app inputs """

        # plot real vs clustered
        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 20), dpi=80, facecolor='w', edgecolor='k')

        xx = np.arange(0,len(self.input_workload.LogData))
        n_time_signals_in = len(self.ts_names)

        for tt in np.arange(0, n_time_signals_in):

            ts_name = self.ts_names[tt]

            ts_sums_input=[]
            for i_app in self.input_workload.LogData:
                if self.job_signal_dgt[tt] == 'sum':
                    ts_sums_input.append(i_app.timesignals[tt].sum)
                elif self.job_signal_dgt[tt] == 'mean':
                    ts_sums_input.append(i_app.timesignals[tt].mean)
                else:
                    raise KeyError("key must be either sum or mean")

            ts_sums_out = [i_ker[ts_name]
                           for i_app in sorted(sa_list)
                           for i_bin in sa_list[i_app]['frames']
                           for i_ker in i_bin
                           if i_ker.has_key(ts_name)]

            # print "--------"
            # print len(xx)
            # print len(ts_sums_input)
            # print len(ts_sums_out)
            # print ts_sums_input

            plt.subplot(n_time_signals_in, 1, tt + 1)
            plt.plot(xx, ts_sums_input, color='b', linestyle='-')
            plt.plot(xx, ts_sums_out, color='r', linestyle='', marker='*')

            if tt == 0:
                plt.title('Sanity check: json input -> json synthetic apps (total values, sc=1)')
            plt.legend(['input from profiler', 'output to synthetic apps'], loc=1)
            plt.ylabel(ts_name)
            plt.xlabel('#Apps')
        plt.savefig(self.out_dir + '/' 'Sanity_check.png')
        plt.close(i_fig)
