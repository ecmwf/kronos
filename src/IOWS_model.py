import json
import os
import numpy as np
import matplotlib.pyplot as plt
import random

# from sklearn.decomposition import PCA
from exceptions_iows import ConfigurationError
from plot_handler import PlotHandler
from time_signal import TimeSignal
from synthetic_app import SyntheticApp, SyntheticWorkload
import time_signal
import model_workload
from tools.print_colour import print_colour

import clustering


from tools import mytools


class IOWSModel(object):

    """
    A model of the workload that applies:

      1) clustering of the apps
      2) visualization
      3) scaling factor and redeployment of synthetic apps
    """

    # A lookup for how to treat different clustering methods
    clustering_routines = {
        'spectral': 'apply_spectral_clustering',
        'time_plane': 'apply_time_plane_clustering',
        'none': 'apply_no_clustering'
    }

    def __init__(self, config_options, workload):

        assert isinstance(workload, model_workload.ModelWorkload)
        self.input_workload = workload
        self.config = config_options

        self.synth_apps = []
        self.schedule = []

        self.out_dir = config_options.dir_output

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

        self.aggregated_metrics = []
        self._n_clusters = None
        self.syntapp_list = []

        self.clustering_key = None

        # num of kernels for each synth app, temporarily set as constant (TODO: to change..)
        self.n_kernels = config_options.WORKLOADCORRECTOR_JOBS_NBINS
        self.n_time_signals = len(time_signal.signal_types)
        self.ts_names = time_signal.signal_types.keys()

        # Some clustering configuration
        # TODO: Validate config at this stage too
        self.scaling_factor = config_options.model_scaling_factor
        self.clustering_algorithm = config_options.model_clustering_algorithm
        self.clustering_routine = config_options.model_clustering

        self.cpu_frequency = config_options.CPU_FREQUENCY
        self.config_options = config_options

        # A quick sanity check
        for method_name in self.clustering_routines.values():
            assert getattr(self, method_name, None)

    @staticmethod
    def validate_config(config):
        """
        Provide immediate feedback to the Config object, so that errors can be reported at startup time
        not later on.
        """
        if config.model_clustering_key not in IOWSModel.clustering_routines:
            raise ConfigurationError("Clustering type not known: {}".format(config.model_clustering_key))

        if config.model_clustering_key == "none":
            if config.model_clustering_algorithm is not None:
                raise ConfigurationError("Clustering algorithm not required for no-clustering")
        else:
            if config.model_clustering_algorithm not in clustering.clustering_algorithms:
                raise ConfigurationError(
                    "Clustering algorithm not recognised: {}".format(config.model_clustering_algorithm))

        if config.model_scaling_factor <= 0.0 or config.model_scaling_factor > 1.0:
            raise ConfigurationError("Scaling factor must be between 0.0 and 1.0")

    def create_scaled_workload(self, clustering_key, which_clust=None, scaling_factor_dict=None, reduce_jobs_flag=True):
        """
        Create a scaled workload of synthetic apps from all of the (so-far) aggregated data
        """
        # Provide default scaling factors of 1.0
        sf_tmp = {k: 1.0 for k in time_signal.signal_types}
        if scaling_factor_dict is not None:
            sf_tmp.update(scaling_factor_dict)
        scaling_factor_dict = sf_tmp

        which_clust = which_clust or self.clustering_algorithm
        scaling_factor = float(sum(scaling_factor_dict.values())) / float(len(scaling_factor_dict.values()))
        scaling_factor = scaling_factor or self.scaling_factor

        assert scaling_factor > 0.

        if scaling_factor > 1.:
            print_colour("orange", "high scaling_factor value! sc={}".format(scaling_factor))

        # call the clustering first..
        print "Apply clustering: {}".format(clustering_key)
        print "Apply clustering: {}".format(which_clust)
        self.apply_clustering(clustering_key, which_clust, reduce_jobs_flag, scaling_factor)
        self.calculate_total_metrics()

        print "Scaling factor: ", scaling_factor

        # ---------- append jobs from clusters to synthetic app list ------------
        n_bins = len(self.input_workload.job_list[0].timesignals[self.ts_names[0]].xvalues_bins)

        maxStartTime_fromT0 = self.input_workload.maxStartTime_fromT0

        # Calculate sums of total metrics signals (of the original WL)
        tot_metrics_sums = np.array([sum(signal.yvalues) for signal in self.input_workload.total_metrics])

        # Calculate sums of synthetic (needed for rescaling each ts to match target scaling factor)
        clust_metrics_sums = np.array([sum(metric.yvalues) for metric in self.aggregated_metrics])

        syntapp_list = []
        for iC in range(0, self._n_clusters):

            # Assemble the time-series
            synapp_timeseries = {}
            for i, ts_name in enumerate(self.ts_names):

                idx_ts = np.arange(0, n_bins) + n_bins * i
                ts_signal_x = np.arange(0, len(self.cluster_centers[iC, idx_ts])) * 1.0  # just a dummy value (not actually needed..)

                if clust_metrics_sums[i]:
                    ts_scaling = scaling_factor_dict[ts_name]
                    ts_signal_y = (self.cluster_centers[iC, idx_ts] * tot_metrics_sums[i] * ts_scaling /
                                   clust_metrics_sums[i])
                else:
                    ts_signal_y = np.zeros(len(ts_signal_x))

                # create ts from retrieved_yvals
                ts = TimeSignal.from_values(ts_name, ts_signal_x, ts_signal_y)

                # we cannot re-digitize the clusters as xvalues are not meaningful..
                # ts.digitize(10, 'sum')
                synapp_timeseries[ts_name] = ts

            # When should the jobs start?

            # create app from cluster time signals
            # TODO: ncpus and nnodes are hacked in here. Do it properly
            # TODO: non-random start times
            app = SyntheticApp(
                job_name="appID-{}".format(iC),
                time_signals=synapp_timeseries,
                ncpus=2,
                nnodes=1,
                time_start=random.random() * maxStartTime_fromT0
            )
            syntapp_list.append(app)

        # # ------ finally append non-clustered jobs ----------
        # for (jj, i_job) in enumerate(self.input_workload.LogData):
        #     if i_job.job_impact_index_rel >= self.job_impact_index_rel_tsh:
        #         app = SyntheticApp()
        #         app.job_name = "JOB-NON-clustered-" + str(jj)
        #         app.fill_time_series(i_job.timesignals)
        #         self.syntapp_list.append(app)

        return SyntheticWorkload(self.config, apps=syntapp_list)

    def apply_clustering(self, clustering_key, which_clust, reduce_jobs_flag, scaling_factor):

        self.clustering_key = clustering_key

        # determine the number of clusters (also according to the flag..)
        if reduce_jobs_flag:
            self._n_clusters = max(1, int(scaling_factor * len(self.input_workload.job_list)))
        else:
            self._n_clusters = len(self.input_workload.job_list)

        # do the clustering only if scaling factor is less than 100
        if (self._n_clusters < 100.) and reduce_jobs_flag:

            worker_name = self.clustering_routines.get(clustering_key, None)
            if worker_name is None:
                raise Exception("Clustering method {} not found".format(clustering_key))

            clustering_worker = getattr(self, worker_name, None)
            assert clustering_worker

        else:  # copy jobs directly into clusters list..

            clustering_worker = self.apply_no_clustering

        # And do the clustering
        clustering_worker(which_clust, scaling_factor)

    def calculate_total_metrics(self):
        """ Calculate global metrics for all the jobs """

        # rebuild the centroids signals
        cluster_all_output = np.array([]).reshape(len(self.input_workload.job_list[0].timesignals) + 1, 0)

        for job, label in zip(self.input_workload.job_list, self.cluster_labels):

            block_vec = np.array([]).reshape(0, len(job.timesignals[self.ts_names[0]].xvalues_bins))
            block_vec = np.vstack((block_vec, job.timesignals[self.ts_names[0]].xvalues_bins))

            # retrieve the appropriate time signal from cluster set (only if the impact index is low..)
            # if i_job.job_impact_index_rel <= self.job_impact_index_rel_tsh:
            for (tt, i_ts_clust) in enumerate(job.timesignals):
                idx_ts = np.arange(0, self.jobs_n_bins) + self.jobs_n_bins * tt
                retrieved_yvals = self.cluster_centers[label, idx_ts]
                block_vec = np.vstack((block_vec, retrieved_yvals))

            cluster_all_output = np.hstack((cluster_all_output, block_vec))
        cluster_all_output = cluster_all_output[:, cluster_all_output[0, :].argsort()]

        # calculate the total metrics
        list_signals = []
        for i, signal in enumerate(self.ts_names):
            ts = TimeSignal.from_values(signal, cluster_all_output[0, :], cluster_all_output[i+1, :])
            ts.digitize(self.total_metrics_nbins)
            list_signals.append(ts)

        self.aggregated_metrics = list_signals

    def apply_time_plane_clustering(self, which_clust, scaling_factor=None):
        """
        Use the time-plane clustering method
        :param which_clust: Specify the form of clustering to use. See clustering/__init__.py
        :param scaling_factor: The fraction of jobs to retain
        :return:
        """

        # NOTE: n_signals assumes that all the jobs signals have the same num of signals
        # NOTE: n_bins assumes that all the time signals have the same
        # num of points
        data = np.array([]).reshape(0, self.jobs_n_bins * self.n_time_signals)

        for job in self.input_workload.job_list:
            data_row = np.concatenate([job.timesignals[signal].yvalues_bins for signal in self.ts_names])
            data = np.vstack((data, data_row))

        # clustering
        cluster_method = clustering.factory(which_clust, data)
        cluster_method.train_method(self._n_clusters, self.kmeans_maxiter)
        self.cluster_centers = cluster_method.clusters
        self.cluster_labels = cluster_method.labels


        # # calculate 2D PCA
        # self.pca = PCA(n_components=2).fit(self.cluster_centers)
        # self.pca_2d = self.pca.transform(self.cluster_centers)

    def apply_no_clustering(self, which_clust, scaling_factor=None):
        """
        A straight-through "clustering" approach (which essentially does nothing).
        """

        # NOTE: n_signals assumes that all the jobs signals have the same num of signals
        # NOTE: n_bins assumes that all the time signals have the same
        # num of points

        # for job in self.input_workload.job_list:
        #     for ts_name, ts in job.timesignals.iteritems():
        #         print "{} ---- {} -- {}".format(ts_name, sum(ts.yvalues), ts.yvalues)
        #         print "{} ---- {} -- {}".format(ts_name, sum(ts.yvalues_bins), ts.yvalues_bins)

        data = np.vstack((
            np.concatenate([job.timesignals[signal].yvalues_bins for signal in self.ts_names])
            for job in self.input_workload.job_list
        ))

        self.cluster_centers = data
        self.cluster_labels = np.arange(0, len(self.input_workload.job_list))

    def apply_spectral_clustering(self, which_clust, scaling_factor=None):
        """
        Use the spectral clustering method
        """
        raise NotImplementedError
        # # clustering
        # start = time.clock()
        #
        # # NOTE: n_signals assumes that all the jobs signals have the same num of signals
        # # NOTE: n_bins assumes that all the time signals have the same num of points
        # n_signals = len(self.input_workload.job_list[0].timesignals)
        # n_freq = self.input_workload.job_list[0].timesignals[0].freqs.size
        # data = np.array([]).reshape(0, (n_freq * 3) * n_signals)
        # for i_job in self.input_workload.job_list:
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
        # cluster_all_output = np.array([]).reshape(len(self.input_workload.job_list[0].timesignals) + 1, 0)
        #
        # for (cc, i_job) in enumerate(self.input_workload.job_list):
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
        # ts_names = [i_ts.name for i_ts in self.input_workload.job_list[0].timesignals]
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

    def make_plots(self, plt_tag):
        """ Plotting routine """

        np.random.seed(1)
        color_vec = np.random.rand(3, len(self.input_workload.job_list[0].timesignals))

        # plot real vs clustered
        i_fig = PlotHandler.get_fig_handle_ID()
        plt.figure(i_fig, figsize=(12, 20), dpi=80, facecolor='w', edgecolor='k')
        plt.title('time series ' + 'cluster_NC=' + str(self._n_clusters))
        tot_signals = self.aggregated_metrics

        for (cc, i_ts) in enumerate(self.input_workload.job_list[0].timesignals):
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

        for (cc, i_ts) in enumerate(self.input_workload.job_list[0].timesignals):
            plt.subplot(len(tot_signals), 1, cc + 1)
            # xx = self.input_workload.total_metrics[cc].xedge_bins[:-1]
            yy = self.input_workload.total_metrics[cc].yvalues_bins
            # tt_dx = self.input_workload.total_metrics[cc].dx_bins
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

        xx = np.arange(0,len(self.input_workload.job_list))
        n_time_signals_in = len(self.ts_names)

        for tt in np.arange(0, n_time_signals_in):

            ts_name = self.ts_names[tt]

            ts_sums_input = []
            for i_app in self.input_workload.job_list:
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
