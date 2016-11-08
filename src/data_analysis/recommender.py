import os
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from sklearn import cross_validation as cv

from data_analysis import jobs_to_matrix, apply_matrix_to_jobs

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from time_signal import signal_types
from kronos_tools.print_colour import print_colour


class Recommender(object):

    def __init__(self):
        self.input_jobs = None
        self.train_test_ratio = None
        self.item_max_train = None
        self.user_similarity_matrix = None
        self.item_similarity_matrix = None

    def train_model(self, input_jobs=None, train_test_ratio=0.8):
        """
        Train model with input model jobs
        :param input_jobs: list of model jobs
        :param n_bins_per_job: N of bins for each job metric to be used for training
        :param train_test_ratio: ratio between training and testing set
        :return:
        """

        self.input_jobs = input_jobs
        self.train_test_ratio = train_test_ratio

        print_colour("green", "Training recommender system..")

        input_jobs_matrix = jobs_to_matrix(input_jobs)

        # Normalize all the data according to their maximum value
        self.item_max_train = np.max(input_jobs_matrix, axis=0)
        input_jobs_matrix_norm = input_jobs_matrix / (self.item_max_train[None, :] + 1.e-10)

        # calculate cosine similarities (user-user matrix and item-item similarity matrix)..
        # these similarity matrices will then be used to predict the missing data for "new users" (jobs..)
        train_data_matrix, test_data_matrix = cv.train_test_split(input_jobs_matrix_norm,
                                                                  test_size=1.-train_test_ratio)
        self.user_similarity_matrix = pairwise_distances(train_data_matrix, metric='cosine')
        self.item_similarity_matrix = pairwise_distances(train_data_matrix.T, metric='cosine')

        print "user-item matrix size = {}".format(self.user_similarity_matrix.shape)
        print "item-item matrix size = {}".format(self.item_similarity_matrix.shape)

    def apply_model_to(self, production_jobs=None):
        """
        Apply recommender system to accounting jobs..
        """

        if not production_jobs:
            raise ValueError("production_jobs not set")

        if not isinstance(production_jobs, list):
            raise ValueError("production_jobs not a list")

        print_colour("green", "Applying recommender system..")

        # Add the accounting job records
        production_jobs_vec = jobs_to_matrix(production_jobs)

        # Apply recommender system values:
        production_jobs_vec_norm = production_jobs_vec / (self.item_max_train[None, :] + 1.e-10)
        item_prediction_norm = production_jobs_vec_norm.dot(self.item_similarity_matrix) / \
                                   np.array([np.abs(self.item_similarity_matrix).sum(axis=1)])

        item_prediction = item_prediction_norm[:, :len(signal_types)] * self.item_max_train[None, :len(signal_types)]
        item_prediction = np.hstack((item_prediction, production_jobs_vec[:, len(signal_types):]))

        production_jobs = apply_matrix_to_jobs(production_jobs, item_prediction)

        return production_jobs