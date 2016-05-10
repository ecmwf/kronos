
class ClusteringBase(object):
    """
    Base class, defining structure for clustering algorithms
    """
    def __init__(self, inputdata):

        self.labels = None
        self.clusters = None
        self._inputdata = inputdata

    def train_method(self, nclust, maxiter):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    # def apply_method(self):
        # print "base class: apply_method"
