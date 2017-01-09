import numpy as np
from sklearn.cluster import DBSCAN
from clust_base import ClusteringBase
from exceptions_iows import ConfigurationError


class ClusteringDBSCAN(ClusteringBase):
    """
    Implement DBSCAN data_analysis algorithms
    """

    def __init__(self, config):

        self.apply_to = None
        self.type = None
        self.rseed = None
        self.max_num_clusters = None

        # Then set the general configuration into the parent class..
        super(ClusteringDBSCAN, self).__init__(config)

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """
        for k, v in self.config.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected ClusteringDBSCAN keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

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
