from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs

from ClusteringBase import ClusteringBase


class ClusteringKmeans(ClusteringBase):

    """Kmeans Class for clustering algorithms"""

    #================================================================
    def __init__(self, inputdata):

        #------ load in input data -------
        ClusteringBase.__init__(self, inputdata)

    #================================================================
    def train_method(self, nclust, maxiter, rseed=170):

        y_pred = KMeans(n_clusters=nclust, max_iter=maxiter,
                        random_state=rseed).fit(self._inputdata)

        self.clusters = y_pred.cluster_centers_
        self.labels = y_pred.labels_
        
        return nclust

    # ================================================================
    # def apply_method(self):

        #self.cluster_centers[iNC] = y_pred.cluster_centers_
        #self.cluster_labels[iNC]  = y_pred.labels_
