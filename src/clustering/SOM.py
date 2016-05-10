from base import ClusteringBase
from tools import *


class ClusteringSOM(ClusteringBase):
    """
    SOM Class for clustering algorithms
    """

    def train_method(self, nclusters, maxiter):

        (nclustX, nclustY) = squarest_pair(nclusters)

        # Initialise training
        som_col = nclustX
        som_row = nclustY
        input_vector = self._inputdata
        input_vector_len = len(input_vector[0, :])

        # Initial effective width
        sigma_initial = 0.8

        # TODO: pass these parameters through config file..

        # Time constant for sigma
        t1 = maxiter * 2.0

        # initial learning rate
        initial_learning_rate = 0.1

        # time constant for eta
        t2 = maxiter * 2.0

        # Assign random weight vectors for all the neurons
        som_map = np.random.rand(som_row, som_col, input_vector_len)

        # Initialise matrices for indexing
        mat_idxs_g = np.mgrid[0:som_row, 0:som_col]
        mat_idxs = np.zeros((som_row, som_col, 2))
        mat_idxs[:, :, 0] = mat_idxs_g[0, :, :]
        mat_idxs[:, :, 1] = mat_idxs_g[1, :, :]

        # Do the training...

        count = 1
        while count < maxiter:

            sigma = sigma_initial * np.exp(-count / t1)
            variance = sigma ** 2.0
            eta = initial_learning_rate * np.exp(-float(count) / float(t2))

            # Randomly select an input_vector sample
            input_index = np.random.randint(len(input_vector[:, 0]))
            selected_input_sample = input_vector[input_index, :]

            # Select the winning neuron (= neuron closest to weight vector)
            dist = np.linalg.norm(selected_input_sample - som_map, axis=2)
            minc = np.argmin(np.amin(dist, axis=0))
            minr = np.argmin(np.amin(dist, axis=1))

            # compute the neighbourhood function for all the neurons
            # TODO rewrite this properly (using "tile")..
            mat_rc = np.ones((som_row, som_col, 2))
            mat_rc[:, :, 0] *= minr
            mat_rc[:, :, 1] *= minc
            distance_tmp = np.linalg.norm(mat_idxs - mat_rc, axis=2)
            neighbourhood_function_val = np.exp(-distance_tmp / (2 * variance))

            # TODO make this block vectorized..
            old_weight_vector = som_map
            for row in range(0, som_row):
                for col in range(0, som_col):
                    som_map[row, col, :] = old_weight_vector[row, col, :] + \
                        eta * neighbourhood_function_val[row, col] * (
                            selected_input_sample - old_weight_vector[row, col, :])

            # counter
            count += 1

        # Apply SOM on input data
        self.clusters = som_map.reshape(nclustX * nclustY, input_vector_len)
        self.labels = np.array([])

        # Retrieve winner neurons
        for row in range(self._inputdata.shape[0]):
            input_sample = self._inputdata[row, :]
            tiled_in = np.tile(input_sample, (nclustX * nclustY, 1))
            dist = np.linalg.norm(tiled_in - self.clusters, axis=1)
            minr = np.argmin(dist)
            self.labels = np.append(self.labels, minr)

        return nclusters