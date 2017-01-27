# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
from sklearn.cluster import DBSCAN
from clust_base import ClusteringBase


class ClusteringDBSCAN(ClusteringBase):
    """
    Implement DBSCAN data_analysis algorithms
    """

    required_config_fields = [
        'apply_to',
        'type',
        'rseed',
        'max_num_clusters'
    ]

    def __init__(self, config):

        self.apply_to = None
        self.type = None
        self.rseed = None
        self.max_num_clusters = None

        # Then set the general configuration into the parent class..
        super(ClusteringDBSCAN, self).__init__(config)

    def apply_clustering(self, input_matrix):

        print "calculating clusters by DBSCAN.."

        eps_n_max = -10
        n_max = -10
        for eps_exp in range(self.max_num_clusters):
            eps = 2**eps_exp
            db = DBSCAN(eps=eps, min_samples=2).fit(input_matrix)
            if len(set(db.labels_)) > n_max:
                eps_n_max = eps
                n_max = len(set(db.labels_))

        # re-apply data_analysis with maximum eps
        db = DBSCAN(eps=eps_n_max, min_samples=2).fit(input_matrix)
        self.labels = db.labels_

        # print np.linalg.norm(np.diff(self._inputdata), axis=1)

        # Number of clusters in labels, ignoring noise (if present).
        self.labels = [0 if item==-1 else item for item in self.labels]
        nclusters = len(set(self.labels))

        print 'Estimated number of clusters: {:d}'.format(nclusters)

        # check that there is at least one cluster..
        assert nclusters >= 1

        # Since the clusters can be convex, cluster points are calculated by minimum distance point
        # TODO to use some more sensible measure of the clusters centroids...
        self.clusters = np.array([]).reshape(0, input_matrix.shape[1])
        for iC in range(0, nclusters):
            pts = input_matrix[np.asarray(self.labels) == iC, :]
            nn = np.linalg.norm(pts, axis=0)
            self.clusters = np.vstack((self.clusters, nn))

        return self.clusters, db.labels_
