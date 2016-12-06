import os
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances

from data_analysis import jobs_to_matrix, apply_matrix_to_jobs

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from kronos_tools.print_colour import print_colour


class Recommender(object):

    def __init__(self):
        self.input_jobs = None
        self.train_test_ratio = None
        self.item_max_train = None
        self.item_simil_mat = None
        self.input_jobs_matrix = None
        self.n_ts_bins = None
        self.col_idxs = None

    def train_model(self, input_jobs=None, train_test_ratio=0.8, n_ts_bins=1):
        """
        Train model with input model jobs
        :param input_jobs: list of model jobs
        :param n_bins_per_job: N of bins for each job metric to be used for training
        :param train_test_ratio: ratio between training and testing set
        :return:
        """

        self.input_jobs = input_jobs
        self.train_test_ratio = train_test_ratio
        self.n_ts_bins = n_ts_bins

        print_colour("green", "Training recommender system..")

        # self.input_jobs_matrix, self.col_idxs = jobs_to_matrix(input_jobs, n_ts_bins=self.n_ts_bins)
        self.input_jobs_matrix = jobs_to_matrix(input_jobs, n_ts_bins=self.n_ts_bins)

        # item-item similarity matrix (metrics only)
        self.item_max_train = np.max(self.input_jobs_matrix, axis=0)
        mat_norm = self.input_jobs_matrix / (self.item_max_train[None, :] + 1.e-20)
        self.item_simil_mat = -pairwise_distances(mat_norm.T, metric='cosine')+1

        # np.set_printoptions(formatter={'float': '{: 0.3f}'.format}, linewidth=200)
        # print "recomm_sys.input_jobs_matrix", self.input_jobs_matrix
        # print "self.item_max_train :", self.item_max_train

    def apply_model_to(self, jobs_to_fill, priority):
        """
        Apply recommender system to accounting jobs..
        """

        if not isinstance(jobs_to_fill, list):
            raise ValueError("jobs_to_fill not a list")

        print_colour("green", "Applying recommender system..")

        # Add the accounting job records
        metrics_mat = jobs_to_matrix(jobs_to_fill, n_ts_bins=self.n_ts_bins)

        # Apply recommender system values:
        metrics_mat_norm = metrics_mat / (self.item_max_train[None, :] + 1.e-16)
        item_prediction_norm = metrics_mat_norm.dot(self.item_simil_mat) / \
                               np.array([np.abs(self.item_simil_mat).sum(axis=1)])
        item_prediction = item_prediction_norm * self.item_max_train

        # filled_jobs = apply_matrix_to_jobs(jobs_to_fill,
        #                                    item_prediction,
        #                                    col_idxs,
        #                                    self.n_ts_bins,
        #                                    priority)

        filled_jobs = apply_matrix_to_jobs(jobs_to_fill,
                                           item_prediction,
                                           priority)

        return filled_jobs
