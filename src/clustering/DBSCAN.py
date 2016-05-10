import numpy as np
from sklearn.cluster import DBSCAN
from base import ClusteringBase


class ClusteringDBSCAN(ClusteringBase):
    """
    Implement DBSCAN clustering algorithms
    """

    # Uses constructor from Clustering base

    def train_method(self, nclust, maxiter):

        db = DBSCAN(eps=1000.0, min_samples=2).fit(self._inputdata)
        self.labels = db.labels_
        print np.linalg.norm(np.diff(self._inputdata), axis=1)

        # Number of clusters in labels, ignoring noise (if present).
        nclusters = len(set(self.labels)) - (1 if -1 in self.labels else 0)
#        self.labels = np.reshape(self.labels,(1,690))

        print('Estimated number of clusters: %d' % nclusters)

        # Since the clusters can be convex, cluster points are calculated by minimum distance point
        # TODO retrieve some sensible measure of the clusters...
        self.clusters = np.array([]).reshape(0,self._inputdata.shape[1])  
        for iC in range(0, nclusters):
            pts = self._inputdata[self.labels == iC, :]
            self.clusters = np.vstack((self.clusters, np.linalg.norm(pts, axis=0)))

        return nclusters
