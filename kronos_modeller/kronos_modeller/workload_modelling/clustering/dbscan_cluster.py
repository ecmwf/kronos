import logging

import numpy as np
from kronos_modeller.kronos_tools.gyration_radius import r_gyration
from kronos_modeller.workload_modelling.clustering.clustering_strategy import ClusteringStrategy
from sklearn.cluster import DBSCAN

logger = logging.getLogger(__name__)


class KmeansClusters(ClusteringStrategy):

    """
    Apply job metrics from a name matching job
    """

    required_config_fields = ClusteringStrategy.required_config_fields + \
                             [
            # NOTE: No additional params needed for DBSCAN
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

            logger.info("----> applying clustering to workload {}".format(wl_entry))

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
        do apply the clustering
        :param input_matrix:
        :param config:
        :return:
        """

        # This internal function finds the clusters
        logger.info("calculating clusters by DBSCAN..")

        eps_n_max = -10
        n_max = -10
        for eps_exp in range(config["max_num_clusters"]):
            eps = 2 ** eps_exp
            db = DBSCAN(eps=eps, min_samples=2).fit(input_matrix)
            if len(set(db.labels_)) > n_max:
                eps_n_max = eps
                n_max = len(set(db.labels_))

        # re-apply data_analysis with maximum eps
        db = DBSCAN(eps=eps_n_max, min_samples=2).fit(input_matrix)
        labels = db.labels_

        # print np.linalg.norm(np.diff(self._inputdata), axis=1)

        # Number of clusters in labels, ignoring noise (if present).
        labels = [0 if item == -1 else item for item in labels]
        nclusters = len(set(labels))

        logger.info('Estimated number of clusters: {:d}'.format(nclusters))

        # check that there is at least one cluster..
        assert nclusters >= 1

        # Since the clusters can be convex, cluster points are calculated by minimum distance point
        # TODO to use some more sensible measure of the clusters centroids...
        cluster_matrix = np.array([]).reshape(0, input_matrix.shape[1])
        for iC in range(0, nclusters):
            pts = input_matrix[np.asarray(labels) == iC, :]
            nn = np.linalg.norm(pts, axis=0)
            cluster_matrix = np.vstack((cluster_matrix, nn))

        return cluster_matrix, db.labels_
