import numpy as np
from sklearn.cluster import DBSCAN
from base import ClusteringBase


class ClusteringDBSCAN(ClusteringBase):
    """
    Implement DBSCAN clustering algorithms
    """

    # Uses constructor from Clustering base

    def train_method(self, nclust, maxiter):

        print "calculating clusters by DBSCAN.."

        eps_n_max = -10
        n_max = -10
        for eps_exp in range(30):
            eps = 2**eps_exp
            db = DBSCAN(eps=eps, min_samples=2).fit(self._inputdata)
            if len(set(db.labels_)) > n_max:
                eps_n_max = eps
                n_max = len(set(db.labels_))

        # re-apply clustering with maximum eps
        db = DBSCAN(eps=eps_n_max, min_samples=2).fit(self._inputdata)
        self.labels = db.labels_

        # print np.linalg.norm(np.diff(self._inputdata), axis=1)

        # Number of clusters in labels, ignoring noise (if present).
        self.labels = [0 if item==-1 else item for item in self.labels]
        nclusters = len(set(self.labels))

        # nclusters = len(set(self.labels)) - (1 if -1 in self.labels else 0)
#        self.labels = np.reshape(self.labels,(1,690))

        print 'Estimated number of clusters: {:d}'.format(nclusters)

        # check that there is at least one cluster..
        assert nclusters >= 1

        # Since the clusters can be convex, cluster points are calculated by minimum distance point
        # TODO retrieve some sensible measure of the clusters...
        clusters = np.array([]).reshape(0, self._inputdata.shape[1])
        for iC in range(0, nclusters):
            pts = self._inputdata[np.asarray(self.labels) == iC, :]
            nn = np.linalg.norm(pts, axis=0)
            clusters = np.vstack((clusters, nn))

        self.clusters = clusters

        return nclusters
