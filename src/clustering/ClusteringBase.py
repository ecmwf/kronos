

class ClusteringBase(object):

    """Base Class for clustering algorithms"""

    #================================================================
    def __init__(self, inputdata):

        self._inputdata = inputdata

    #================================================================
    def train_method(self, nclust, maxiter):

        print "base class: train_method" 
        return 0

    # ================================================================
    # def apply_method(self):

        # print "base class: apply_method"
