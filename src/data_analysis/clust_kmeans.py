import numpy as np
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans

from clust_base import ClusteringBase
from data_analysis.elbow_method import find_n_clusters
from kronos_tools.print_colour import print_colour
from exceptions_iows import ConfigurationError


class ClusteringKmeans(ClusteringBase):
    """
    Kmeans Class for data_analysis algorithms
    """

    def __init__(self, config):

        self.type = None
        self.ok_if_low_rank = None
        self.user_does_not_check = None
        self.rseed = None
        self.max_iter = None
        self.apply_to = None
        self.nc_max = None
        self.nc_delta = None

        # Then set the general configuration into the parent class..
        super(ClusteringKmeans, self).__init__(config)

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """
        for k, v in self.config.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected ClusteringKmeans keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def apply_clustering(self, input_matrix):

        print_colour("green", "calculating clusters by Kmeans..")

        nc_vec = np.arange(1, self.nc_max, self.nc_delta)
        avg_d_in_clust = np.zeros(nc_vec.shape[0])
        n_clusters_max = None

        for cc, n_clusters in enumerate(nc_vec):
            print_colour("white", "Doing K-means with {} clusters, matrix size={}".format(n_clusters, input_matrix.shape))

            if n_clusters > input_matrix.shape[0]:
                print_colour("orange", "N clusters > matrix row size! => max n clusters = {}".format(n_clusters-1))
                n_clusters_max = n_clusters-1
                break
            else:
                y_pred = KMeans(n_clusters=n_clusters,
                                max_iter=self.max_iter,
                                random_state=self.rseed
                                ).fit(input_matrix)

                clusters = y_pred.cluster_centers_
                # labels = y_pred.labels_
                pt_to_all_clusters = cdist(input_matrix, clusters, 'euclidean')
                dist_in_c = np.min(pt_to_all_clusters, axis=1)
                avg_d_in_clust[cc] = np.mean(dist_in_c)

        # Calculate best number of clusters by elbow_method
        if not n_clusters_max:
            n_clusters_optimal = find_n_clusters(avg_d_in_clust, self.user_does_not_check)
        else:
            n_clusters_optimal = n_clusters_max

        y_pred = KMeans(n_clusters=n_clusters_optimal,
                        max_iter=self.max_iter,
                        random_state=self.rseed
                        ).fit(input_matrix)

        print_colour("white", "Optimal number of clusters: {}".format(n_clusters_optimal))
        return y_pred.cluster_centers_, y_pred.labels_

