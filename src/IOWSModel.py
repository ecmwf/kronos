from sklearn.decomposition import PCA
from PlotHandler import PlotHandler
from TimeSignal import TimeSignal

from tools import *
import clustering
import time


class IOWSModel(object):
    """
    A model of the workload that applies:

      1) clustering of the apps
      2) visualization
      3) scaling factor and redeployment of synthetic apps (TODO)
    """

    #===================================================================
    def __init__(self, ConfigOptions):

        self.input_workload = []
        self.synth_apps = []
        self.schedule = []

        self.out_dir = ConfigOptions.DIR_OUTPUT

        self.total_metrics_nbins = ConfigOptions.IOWSMODEL_TOTAL_METRICS_NBINS
        self.kmeans_maxiter = ConfigOptions.IOWSMODEL_KMEANS_MAXITER
        self.kmeans_rseed = ConfigOptions.IOWSMODEL_KMEANS_KMEANS_RSEED
        self.job_impact_index_rel_tsh = ConfigOptions.IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH

        self.bin_means_total_time = {}
        self.bin_means_total_IO_N_read = {}
        self.bin_means_total_IO_N_write = {}
        self.bin_means_total_IO_Kb_read = {}
        self.bin_means_total_IO_Kb_write = {}

        self.cluster_centers = {}
        self.cluster_labels = {}
        self.pca = {}
        self.pca_2d = {}

        self.aggregated_metrics = {}
        self._Nclusters = None

    #===================================================================
    def set_input_workload(self, input_workload):
        self.input_workload = input_workload

    #===================================================================
    def apply_clustering(self, clustering_key, which_clust, iNC=-1):

        self.clustering_key = clustering_key
        self._Nclusters = iNC

        if (clustering_key == "spectral"):

            #======================= clustering =========================
            start = time.clock()

            # NOTE: N_signals assumes that all the jobs signals have the same num of signals
            # NOTE: N_bins assumes that all the time signals have the same num of points
            N_signals = len(self.input_workload.LogData[0].timesignals)
            N_freq = self.input_workload.LogData[0].timesignals[0].freqs.size
            data = np.array([]).reshape(0, (N_freq * 3) * N_signals)
            for ijob in self.input_workload.LogData:
                data_row = []
                for iSign in ijob.timesignals:
                    data_row = hstack((data_row, iSign.freqs.tolist() + iSign.ampls.tolist() + iSign.phases.tolist()))

                data = np.vstack((data, data_row))

            #-------------- clustering ---------------
            cluster_method = clustering.factory(which_clust, data)
            self._Nclusters = cluster_method.train_method(self._Nclusters, self.kmeans_maxiter)            
            self.cluster_centers = cluster_method.clusters
            self.cluster_labels = cluster_method.labels
            
            print "self.cluster_centers.shape ", self.cluster_centers.shape
            #-----------------------------------------

            #-------------------------- calculate 2D PCA ------------------
            self.pca = PCA(n_components=2).fit(self.cluster_centers)
            self.pca_2d = self.pca.transform(self.cluster_centers)
            #--------------------------------------------------------------

            #================ rebuild the centroids signals ===============
            idx1 = np.arange(0, N_freq)
            cluster_all_output = np.array([]).reshape(len(self.input_workload.LogData[0].timesignals) + 1, 0)

            for cc, ijob in enumerate(self.input_workload.LogData):

                #--- load the time of this job signal..
                iC = self.cluster_labels[cc]
                job_times = ijob.timesignals[0].xvalues
                block_vec = np.array([]).reshape(0, len(job_times))
                block_vec = vstack((block_vec, job_times))

                #---- retrieve the appropriate time signal from cluster set (only if the impact index is low..)----
                if ijob.job_impact_index_rel <= self.job_impact_index_rel_tsh:
                    for tt, iTSclust in enumerate(ijob.timesignals):
                        idx_f = idx1 + N_freq * 0 + (N_freq * 3 * tt)
                        idx_a = idx1 + N_freq * 1 + (N_freq * 3 * tt)
                        idx_p = idx1 + N_freq * 2 + (N_freq * 3 * tt)
                        retrieved_f = self.cluster_centers[iC, idx_f]
                        retrieved_a = self.cluster_centers[iC, idx_a]
                        retrieved_p = self.cluster_centers[iC, idx_p]
                        yy = abs(freq_to_time(job_times - job_times[0], retrieved_f, retrieved_a, retrieved_p))
                        block_vec = vstack((block_vec, yy))
                else:
                    print "ijob:", cc, " is not clustered.."
                    for tt, iTSclust in enumerate(ijob.timesignals):
                        yy = iTSclust.yvalues
                        block_vec = vstack((block_vec, yy))

                cluster_all_output = hstack((cluster_all_output, block_vec))
            cluster_all_output = cluster_all_output[:, cluster_all_output[0, :].argsort()]

            #==================== calculate the total metrics =============
            ts_names = [
                iTS.name for iTS in self.input_workload.LogData[0].timesignals]
            list_signals = []

            for iS in range(0, len(ts_names)):
                TS = TimeSignal()
                TS.create_ts_from_values(ts_names[iS], cluster_all_output[0, :], cluster_all_output[iS + 1, :])
                TS.digitize(self.total_metrics_nbins, 'sum')
                list_signals.append(TS)

            self.aggregated_metrics = list_signals

            print "elapsed time: ", time.clock() - start

        elif (clustering_key == "time_plane"):

            #==================================== clustering ==================
            start = time.clock()

            # NOTE: N_signals assumes that all the jobs signals have the same num of signals
            # NOTE: N_bins assumes that all the time signals have the same
            # num of points
            N_signals = len(self.input_workload.LogData[0].timesignals)
            N_bins = len(self.input_workload.LogData[0].timesignals[0].yvalues_bins)

            data = np.array([]).reshape(0, N_bins * N_signals)

            for ijob in self.input_workload.LogData:
                data_row = [item for iSign in ijob.timesignals for item in iSign.yvalues_bins]
                data = np.vstack((data, data_row))

            #-------------- clustering ---------------
            cluster_method = clustering.factory(which_clust, data)
            cluster_method.train_method(self._Nclusters, self.kmeans_maxiter)
            self.cluster_centers = cluster_method.clusters
            self.cluster_labels = cluster_method.labels
            
            print "self.cluster_centers.shape ", self.cluster_centers.shape
            #-----------------------------------------

            #-------------------------- calculate 2D PCA ------------------
            self.pca = PCA(n_components=2).fit(self.cluster_centers)
            self.pca_2d = self.pca.transform(self.cluster_centers)
            #--------------------------------------------------------------

            #============= rebuild the centroids signals ===============
            idx1 = np.arange(0, N_bins)
            cluster_all_output = np.array([]).reshape(len(self.input_workload.LogData[0].timesignals) + 1, 0)

            for cc, ijob in enumerate(self.input_workload.LogData):

                iC = self.cluster_labels[cc]
                block_vec = np.array([]).reshape(0, len(ijob.timesignals[0].xvalues_bins))
                block_vec = vstack((block_vec, ijob.timesignals[0].xvalues_bins))

                #---- retrieve the appropriate time signal from cluster set (only if the impact index is low..)----
                if ijob.job_impact_index_rel <= self.job_impact_index_rel_tsh:
                    for tt, iTSclust in enumerate(ijob.timesignals):
                        idx_ts = np.arange(0, N_bins) + N_bins * tt

                        #from IPython.core.debugger import Tracer
                        # Tracer()()

                        retrieved_yvals = self.cluster_centers[iC, idx_ts]
                        block_vec = vstack((block_vec, retrieved_yvals))
                else:
                    print "ijob:", cc, " is not clustered.."
                    for tt, iTSclust in enumerate(ijob.timesignals):
                        retrieved_yvals = iTSclust.yvalues_bins
                        block_vec = vstack((block_vec, retrieved_yvals))

                cluster_all_output = hstack((cluster_all_output, block_vec))
            cluster_all_output = cluster_all_output[:, cluster_all_output[0, :].argsort()]

            #=================== calculate the total metrics ==============
            ts_names = [iTS.name for iTS in self.input_workload.LogData[0].timesignals]
            list_signals = []

            for iS in range(0, len(ts_names)):
                TS = TimeSignal()
                TS.create_ts_from_values(ts_names[iS], cluster_all_output[0, :], cluster_all_output[iS + 1, :])
                TS.digitize(self.total_metrics_nbins, 'sum')
                list_signals.append(TS)

            self.aggregated_metrics = list_signals

            print "elapsed time: ", time.clock() - start

    #===================================================================
    def make_plots(self, Tag):

        print "plotting.."

        np.random.seed(1)
        colorVec = np.random.rand(3, len(self.input_workload.LogData[0].timesignals))

        #----------------------- plot real vs clutered ------------
        iFig = PlotHandler.get_fig_handle_ID()
        figure(iFig, figsize=(12, 18), dpi=80,facecolor='w', edgecolor='k')
        title('time series ' + 'cluster_NC=' + str(self._Nclusters))
        tot_signals = self.aggregated_metrics

        for cc, iTS in enumerate(self.input_workload.LogData[0].timesignals):
            subplot(len(tot_signals), 1, cc + 1)
            xx = self.input_workload.total_metrics[cc].xedge_bins[:-1]
            yy = self.input_workload.total_metrics[cc].yvalues_bins
            tt_DX = self.input_workload.total_metrics[cc].DX_bins
            bar(xx, yy, tt_DX, color='b')
            bar(tot_signals[cc].xedge_bins[:-1], tot_signals[cc].yvalues_bins, tot_signals[cc].DX_bins, color=colorVec[:, cc])
            legend(['raw data', 'clustered'], loc=2)
            yscale('log')
            ylabel(tot_signals[cc].name)

        xlabel('time [s]')
        savefig(self.out_dir + '/' + Tag + '_plot' +'_cluster_NC=' + str(self._Nclusters) + '.png')
        close(iFig)
        #------------------------------------------------------------------

        #----------------------- plot real vs clutered ERROR --------------
        iFig = PlotHandler.get_fig_handle_ID()
        figure(iFig, figsize=(12, 18), dpi=80,facecolor='w', edgecolor='k')
        title('time series ERROR' + 'cluster_NC=' + str(self._Nclusters))
        tot_signals = self.aggregated_metrics

        for cc, iTS in enumerate(self.input_workload.LogData[0].timesignals):
            subplot(len(tot_signals), 1, cc + 1)
            xx = self.input_workload.total_metrics[cc].xedge_bins[:-1]
            yy = self.input_workload.total_metrics[cc].yvalues_bins
            tt_DX = self.input_workload.total_metrics[cc].DX_bins
            err_vec = [abs((x - y) / y * 100.)for x, y in zip(tot_signals[cc].yvalues_bins, yy)]
            bar(tot_signals[cc].xedge_bins[:-1], err_vec,tot_signals[cc].DX_bins, color=colorVec[:, cc])
            legend(['raw data', 'clustered'], loc=2)
            # yscale('log')
            ylabel(tot_signals[cc].name)

        xlabel('time [s]')
        savefig(self.out_dir + '/' + Tag + '_plot' +'_cluster_NC=' + str(self._Nclusters) + '_ERROR.png')
        close(iFig)
        #------------------------------------------------------------------

        #----------------------- plot PCA centroids.. ---------------------
        if self.pca_2d.shape[1] > 1:
            iFig = PlotHandler.get_fig_handle_ID()
            figure(iFig)
            title('cluster PC''s')
            plot(self.pca_2d[:, 0], self.pca_2d[:, 1], 'bo')
            savefig(self.out_dir + '/' + Tag + '_plot' +'_cluster_PCA=2_' + str(self._Nclusters) + '.png')
            close(iFig)
        #------------------------------------------------------------------
