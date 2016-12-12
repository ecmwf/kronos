
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from exceptions_iows import ConfigurationError

from kronos_tools.print_colour import print_colour


class Recommender(object):

    def __init__(self, input_matrix, n_bins):

        self.input_matrix = input_matrix
        self.n_bins = n_bins

        # check the configuration..
        self.check_input()

    def check_input(self):
        """
        Check configuration..
        :return:
        """

        if self.input_matrix.shape[1] % self.n_bins:
            raise ConfigurationError("matrix col number={} not consistent with n_bins={}in the RS".format(self.input_matrix.shape[1], self.n_bins))

        assert(isinstance(self.input_matrix, np.ndarray))

    def apply(self):
        """
        Train model with input model jobs
        :return:
        """

        print_colour("green", "Training recommender system..")

        # once the matrix is filled, look for missing columns:
        col_size = self.input_matrix.shape[1]
        mapped_columns = []
        for col_idx in range(col_size):
            if (self.input_matrix[:, col_idx] < 0).all():
                pass
            else:
                mapped_columns.append(col_idx)

        working_matrix = self.input_matrix[:, mapped_columns]

        # item-item similarity matrix (metrics only)
        item_max_train = np.max(working_matrix, axis=0)
        mat_norm = working_matrix / (item_max_train[None, :] + 1.e-20)
        item_simil_mat = -pairwise_distances(mat_norm.T, metric='cosine')+1

        item_prediction_norm = mat_norm.dot(item_simil_mat) / (np.array([np.abs(item_simil_mat).sum(axis=1)])+1.e-20)
        filled_matrix = item_prediction_norm * item_max_train

        return filled_matrix, mapped_columns
