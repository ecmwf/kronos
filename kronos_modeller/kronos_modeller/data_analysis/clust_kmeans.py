# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
from kronos_modeller.data_analysis.silhouette import find_n_clusters_silhouette
from kronos_modeller.plot_handler import PlotHandler
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from clust_base import ClusteringBase
from kronos_executor.tools import print_colour


class ClusteringKmeans(ClusteringBase):
    """
    Kmeans Class for data_analysis algorithms
    """

    required_config_fields = [
        'type',
        'ok_if_low_rank',
        'user_does_not_check',
        'rseed',
        'max_iter',
        'apply_to',
        'max_num_clusters',
        'delta_num_clusters'
    ]

    def __init__(self, config):

        self.type = None
        self.ok_if_low_rank = None
        self.user_does_not_check = None
        self.rseed = None
        self.max_iter = None
        self.apply_to = None
        self.max_num_clusters = None
        self.delta_num_clusters = None

        # number of digits to keep for evaluating the max of silhouette plot
        # TODO: to find a better solution for this..
        self.n_round_off = 1

        # Then set the general configuration into the parent class..
        super(ClusteringKmeans, self).__init__(config)

    def apply_clustering(self, input_matrix):

        print_colour("green", "calculating clusters by Kmeans..")

        # check that the max number of clusters is not higher than the n samples in the input matrix
        if self.max_num_clusters >= input_matrix.shape[0]:
            self.max_num_clusters = input_matrix.shape[0]
            print_colour("orange", "N clusters > matrix row size! => max n clusters = {}".format(input_matrix.shape[0]))

        nc_vec = np.arange(2, self.max_num_clusters, self.delta_num_clusters, dtype=int)
        avg_d_in_clust = np.zeros(nc_vec.shape[0])
        silhouette_score_vec = []

        for cc, n_clusters in enumerate(nc_vec):
            print_colour("white", "Doing K-means with {} clusters, matrix size={}".format(n_clusters, input_matrix.shape))

            y_pred = KMeans(n_clusters=int(n_clusters),
                            max_iter=self.max_iter,
                            random_state=self.rseed
                            ).fit(input_matrix)

            clusters = y_pred.cluster_centers_

            silhouette_avg = silhouette_score(input_matrix, y_pred.labels_)
            silhouette_score_vec.append(silhouette_avg)

            # labels = y_pred.labels_
            pt_to_all_clusters = cdist(input_matrix, clusters, 'euclidean')
            dist_in_c = np.min(pt_to_all_clusters, axis=1)
            avg_d_in_clust[cc] = np.mean(dist_in_c)

        # Calculate best number of clusters by silhouette method
        max_s_idx = find_n_clusters_silhouette(silhouette_score_vec, self.n_round_off)
        n_clusters_optimal = nc_vec[max_s_idx]

        y_pred = KMeans(n_clusters=n_clusters_optimal,
                        max_iter=self.max_iter,
                        random_state=self.rseed
                        ).fit(input_matrix)

        print_colour("white", "Optimal number of clusters: {}".format(n_clusters_optimal))

        # stop and plot cluster silhouette values unless specifically requested not to
        if not self.user_does_not_check:
            import matplotlib.pyplot as plt
            plot_handler = PlotHandler()
            plt.figure(plot_handler.get_fig_handle_ID(), facecolor='w', edgecolor='k')
            plt.plot(nc_vec, silhouette_score_vec, 'b')
            plt.scatter(n_clusters_optimal, silhouette_score_vec[max_s_idx], color='r', s=1e2)
            plt.xlabel("# clusters")
            plt.ylabel("Silhouette score")
            plt.show()

        return y_pred.cluster_centers_, y_pred.labels_

