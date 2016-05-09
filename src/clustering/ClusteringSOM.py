import numpy as np
from random import randint
from scipy import random

from tools import *

from ClusteringBase import ClusteringBase


class ClusteringSOM(ClusteringBase):

    """SOM Class for clustering algorithms"""

    #================================================================
    def __init__(self, inputdata):

        #------ load in input data -------
        ClusteringBase.__init__(self, inputdata)

    #================================================================
    def train_method(self, nclust, maxiter):

        (nclustX, nclustY) = squarest_pair(nclust)

        #------------- init mode ------------
        somCol = nclustX
        somRow = nclustY
        input_vector = self._inputdata
        inputvectorlen = len(input_vector[0, :])

        # Initial effective width
        sigmaInitial = 0.8
        #------------------------------------

        #---------- training -------------------
        # TODO: pass these parameters through config file..

        # initialise neurons layer.
        somMap = np.zeros(shape=(somCol, somRow, inputvectorlen))

        # Time constant for sigma
        t1 = maxiter * 2.0

        # Initialize matrix to store neighbourhood functions of each neurons on
        # the map
        neighbourhoodFunctionVal = np.zeros(shape=(somRow, somCol))

        # initial learning rate
        learningRateInitial = 0.1

        # time constant for eta
        t2 = maxiter * 2.0

        # Assign random weight vectors for all the neurons
        somMap = np.random.rand(somRow, somCol, inputvectorlen)

        #----- init matrices for indexing.. ------
        mat_idxs_g = np.mgrid[0:somRow, 0:somCol]
        mat_idxs = np.zeros((somRow, somCol, 2))
        mat_idxs[:, :, 0] = mat_idxs_g[0, :, :]
        mat_idxs[:, :, 1] = mat_idxs_g[1, :, :]
        #-----------------------------------------

        #-------------- training ------------------
        count = 1
        while(count < maxiter):

            sigma = sigmaInitial * np.exp(-count / t1)
            variance = sigma ** 2.0
            eta = learningRateInitial * np.exp(-float(count) / float(t2))

            # Randomly select an input_vector sample
            inputIndex = np.random.randint(len(input_vector[:, 0]))
            selectedInputSample = input_vector[inputIndex, :]

            #==================================================================
            # Select the winning neuron (= neuron closest to weight vector)
            dist = np.linalg.norm(selectedInputSample - somMap, axis=2)
            minc = np.argmin(np.amin(dist, axis=0))
            minr = np.argmin(np.amin(dist, axis=1))
            #==================================================================

            #==================================================================
            # compute the neighbourhood function for all the neurons
            #-- TODO rewrite this properly (using "tile")..
            mat_rc = np.ones((somRow, somCol, 2))
            mat_rc[:, :, 0] *= minr
            mat_rc[:, :, 1] *= minc
            distance_tmp = np.linalg.norm(mat_idxs - mat_rc, axis=2)
            neighbourhoodFunctionVal = np.exp(-distance_tmp / (2 * variance))
            #==================================================================

            #==================================================================
            #-- TODO make this block vectorized..
            oldWeightVector = somMap
            for row in range(0, somRow):
                for col in range(0, somCol):
                    somMap[row, col, :] = oldWeightVector[row, col, :] + \
                        eta * neighbourhoodFunctionVal[row, col] * (
                            selectedInputSample - oldWeightVector[row, col, :])
            #==================================================================

            # counter
            count += 1

        #-------- apply SOM on input data -----------
        self.clusters = somMap.reshape(nclustX * nclustY, inputvectorlen)
        self.labels = np.array([])

        #--------- retrieve winner neurons ----------
        for irow in range(self._inputdata.shape[0]):
            input_sample = self._inputdata[irow, :]
            tiled_in = np.tile(input_sample, (nclustX * nclustY, 1))
            dist = np.linalg.norm(tiled_in - self.clusters, axis=1)
            minr = np.argmin(dist)
            self.labels = np.append(self.labels, minr)
        #--------------------------------------------
            
        return nclust
