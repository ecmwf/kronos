import logging

import numpy as np
from kronos_modeller.job_clustering.clustering_strategy import ClusteringStrategy
from kronos_modeller.job_clustering.silhouette import find_n_clusters_silhouette
from kronos_modeller.kronos_tools.gyration_radius import r_gyration
from kronos_modeller.plot_handler import PlotHandler
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

logger = logging.getLogger(__name__)


class KmeansClusters(ClusteringStrategy):

    """
    Apply job metrics from a name matching job
    """

    required_config_fields = ClusteringStrategy.required_config_fields + \
                             [
                                'ok_if_low_rank',
                                'user_does_not_check',
                                'max_iter',
                                'delta_num_clusters'
                             ]

    def _apply(self, config):

        """
        Apply the kmeans clustering strategy
        :param config:
        :return:
        """

        # # save the KProfile's of the sub-workloads before attempting the clustering
        # save_wl_before_clustering = self.config_job_classification.get(
        # 'save_wl_before_clustering', False)
        # # if save_wl_before_clustering:
        # #     # for wl in self.workloads:
        # #     #     kprofile_hdl = ProfileFormat(model_jobs=wl.jobs, workload_tag=wl.tag)
        # #     #     kprofile_hdl.write_filename(os.path.join(self.config.dir_output,
        #             wl.tag+"_workload.kprofile"))
        # #     wl_group = kronos_modeller.workload_data_group.WorkloadDataGroup(cutout_workloads)
        # #     wl_group.export_pickle(os.path.join(self.config.dir_output, "_workload"))

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation
        # (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        # self._check_jobs(self.config_job_classification['apply_to'])

        # This internal function finds the clusters
        self.do_clustering(config)

    def do_clustering(self, config):

        """
        Actually do the clustering (internal interface)
        :param config:
        :return:
        """

        # loop over all the workloads
        for wl_entry in config['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_entry)

            logger.info("----> applying classification to workload {}".format(wl_entry))

            # Apply clustering
            job_signal_matrix = wl.jobs_to_matrix(config['num_timesignal_bins'])
            (clusters_matrix, clusters_labels) = self.apply_clustering(job_signal_matrix, config)

            # calculate the mean radius of gyration (among all clusters) for each sub-workload
            r_sub_wl_mean = []
            matrix_jobs_in_cluster_all = []
            nbins = config['num_timesignal_bins']
            for cc in range(clusters_matrix.shape[0]):
                matrix_jobs_in_cluster = np.asarray([j.ts_to_vector(nbins) for j in
                                                     np.asarray(wl.jobs)[clusters_labels == cc]])
                r_sub_wl_mean.append(r_gyration(matrix_jobs_in_cluster))
                matrix_jobs_in_cluster_all.append(matrix_jobs_in_cluster)

            self.clusters.append({
                'source-workload': wl_entry,
                'jobs_for_clustering': wl.jobs,
                'cluster_matrix': clusters_matrix,
                'labels': clusters_labels,
                'r_gyration': r_sub_wl_mean,
                'matrix_jobs_in_cluster': matrix_jobs_in_cluster_all
            })

    def apply_clustering(self, input_matrix, config):

        """
        Raw clustering from an input matrix and config params
        :param input_matrix:
        :return:
        """

        logger.info("calculating clusters by Kmeans..")

        n_round_off = 1

        # check that the max number of clusters is not higher than the n samples in the input matrix
        if config["max_num_clusters"] >= input_matrix.shape[0]:
            config["max_num_clusters"] = input_matrix.shape[0]

            logger.info("N clusters > matrix row size! => max n clusters = {}".format(input_matrix.shape[0]))

        nc_vec = np.arange(2, config["max_num_clusters"], config["delta_num_clusters"], dtype=int)
        avg_d_in_clust = np.zeros(nc_vec.shape[0])
        silhouette_score_vec = []

        for cc, n_clusters in enumerate(nc_vec):
            logger.info("Doing K-means with {} clusters, matrix size={}".format(n_clusters, input_matrix.shape))

            y_pred = KMeans(n_clusters=int(n_clusters),
                            max_iter=config["max_iter"],
                            random_state=config["rseed"]
                            ).fit(input_matrix)

            clusters = y_pred.cluster_centers_

            silhouette_avg = silhouette_score(input_matrix, y_pred.labels_)
            silhouette_score_vec.append(silhouette_avg)

            # labels = y_pred.labels_
            pt_to_all_clusters = cdist(input_matrix, clusters, 'euclidean')
            dist_in_c = np.min(pt_to_all_clusters, axis=1)
            avg_d_in_clust[cc] = np.mean(dist_in_c)

        # Calculate best number of clusters by silhouette method
        max_s_idx = find_n_clusters_silhouette(silhouette_score_vec, n_round_off)
        n_clusters_optimal = nc_vec[max_s_idx]

        y_pred = KMeans(n_clusters=n_clusters_optimal,
                        max_iter=config["max_iter"],
                        random_state=config["rseed"]
                        ).fit(input_matrix)

        logger.info("Optimal number of clusters: {}".format(n_clusters_optimal))

        # stop and plot cluster silhouette values unless specifically requested not to
        if not config["user_does_not_check"]:
            import matplotlib.pyplot as plt
            plot_handler = PlotHandler()
            plt.figure(plot_handler.get_fig_handle_ID(), facecolor='w', edgecolor='k')
            plt.plot(nc_vec, silhouette_score_vec, 'b')
            plt.scatter(n_clusters_optimal, silhouette_score_vec[max_s_idx], color='r', s=1e2)
            plt.xlabel("# clusters")
            plt.ylabel("Silhouette score")
            plt.show()

        return y_pred.cluster_centers_, y_pred.labels_