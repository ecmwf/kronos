import numpy as np
from sklearn.cluster import DBSCAN
from ClusteringBase import ClusteringBase


class ClusteringDBSCAN(ClusteringBase):

    """Base Class for clustering algorithms"""

    #================================================================
    def __init__(self, inputdata):

        #------ load in input data -------
        ClusteringBase.__init__(self, inputdata)

    #================================================================
    def train_method(self, nclust, maxiter):

        db = DBSCAN(eps=1000.0, min_samples=2).fit(self._inputdata)
        self.labels = db.labels_
        print np.linalg.norm(np.diff(self._inputdata), axis=1)

        # Number of clusters in labels, ignoring noise (if present).
        n_clusters_ = len(set(self.labels)) - (1 if -1 in self.labels else 0)
#        self.labels = np.reshape(self.labels,(1,690))

        print('Estimated number of clusters: %d' % n_clusters_)

        #--------- since the clusters can be convex, cluster points are ---
        #--------- now calculated by minimum distance point
        # TODO retrieve some sensible measure of the clusters...
        self.clusters = np.array([]).reshape(0,self._inputdata.shape[1])  
        for iC in range(0, n_clusters_):
            pts = self._inputdata[self.labels == iC, :]
            self.clusters = np.vstack( (self.clusters, np.linalg.norm(pts, axis=0) ) )
        #------------------------------------------------------------------

#        from IPython.core.debugger import Tracer            
#        Tracer()()      
            
        #--- N clusters is an output ----
        return n_clusters_            
            
