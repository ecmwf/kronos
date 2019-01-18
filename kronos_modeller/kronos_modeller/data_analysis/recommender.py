# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging
import numpy as np
from kronos_modeller.exceptions_iows import ConfigurationError
from scipy.spatial.distance import pdist

logger = logging.getLogger(__name__)


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

        logger.info( "Training recommender system..")
        np.set_printoptions(edgeitems=1,
                            infstr='inf',
                            linewidth=275,
                            precision=3,
                            suppress=False,
                            formatter=None)

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

        # normalize the matrics and remove non-values (<0)
        mat_norm = working_matrix / (item_max_train[None, :] + 1.e-20)
        mat_norm[mat_norm < 0] = 0

        # item-item similarity matrix (metrics only)
        item_max_train = np.abs(np.max(working_matrix, axis=0))
        mat_norm = working_matrix / (item_max_train[None, :] + 1.e-20)

        # calculate the similarity matrix considering only the pairs of non-missing elements
        item_simil_mat = np.zeros((mat_norm.shape[1], mat_norm.shape[1]))
        for c in range(mat_norm.shape[1]):
            for cc in range(mat_norm.shape[1]):
                valid_rows = np.logical_and(mat_norm[:, c] > 0, mat_norm[:, cc] > 0)
                dist = -pdist(np.vstack((mat_norm[valid_rows==True, c], mat_norm[valid_rows == True, cc])), 'cosine')+1
                item_simil_mat[c, cc] = dist.item(0)

        # retain only max values in similarity matrix
        n_stencil = int(round(item_simil_mat.shape[0] * 3./4.))
        item_simil_mat_stencil = np.copy(item_simil_mat)
        for row in item_simil_mat_stencil:
            ind = np.argpartition(row, -n_stencil)[-n_stencil:]
            not_max_mask = np.ones(row.shape, dtype=bool)
            not_max_mask[ind] = False
            row[not_max_mask] = 0.0

        item_prediction_norm = mat_norm.dot(item_simil_mat_stencil) / (np.array([np.abs(item_simil_mat_stencil).sum(axis=1)])+1.e-20)
        filled_matrix = item_prediction_norm * item_max_train

        return filled_matrix, mapped_columns
