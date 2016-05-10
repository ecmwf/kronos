from sklearn.cluster import KMeans

from base import ClusteringBase


class ClusteringKmeans(ClusteringBase):

    """Kmeans Class for clustering algorithms"""

    # Use ClusteringBase default constructor

    def train_method(self, nclusters, maxiter, rseed=170):

        y_pred = KMeans(n_clusters=nclusters, max_iter=maxiter,
                        random_state=rseed).fit(self._inputdata)

        self.clusters = y_pred.cluster_centers_
        self.labels = y_pred.labels_
        
        return nclusters

    # ================================================================
    # def apply_method(self):

        #self.cluster_centers[iNC] = y_pred.cluster_centers_
        #self.cluster_labels[iNC]  = y_pred.labels_
