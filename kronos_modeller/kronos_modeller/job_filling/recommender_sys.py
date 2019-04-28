import logging

import numpy as np
from scipy.spatial.distance import pdist

from kronos_modeller.job_filling.filling_strategy import FillingStrategy
from kronos_modeller.kronos_exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class StrategyRecommender(FillingStrategy):

    """
    Apply recommender system to jobs of a
    certain workload type
    """

    required_config_fields = FillingStrategy.required_config_fields + \
        [
            'n_bins',
            'apply_to'
        ]

    def _apply(self, config, user_functions):

        for wl_name in config['apply_to']:

            logger.info( "Applying recommender system on workload: {}".format(wl_name))

            wl_dest = next(wl for wl in self.workloads if wl.tag == wl_name)
            wl_dest.apply_recommender_system(config)

    @staticmethod
    def apply_recommender_system(wl_dest, rs_config):
        """
        Apply a recommender system technique to the jobs of this workload
        :param wl_dest:
        :param rs_config:
        :return:
        """

        n_bins = rs_config['n_bins']
        priority = rs_config['priority']

        # get the total matrix fro the jobs
        ts_matrix = wl_dest.jobs_to_matrix(n_bins)

        if ts_matrix.shape[1] % n_bins:
            err_tmpl = "matrix col number={} not consistent with n_bins={}in the RS"
            err = err_tmpl.format(ts_matrix.shape[1], n_bins)
            raise ConfigurationError(err)

        assert (isinstance(ts_matrix, np.ndarray))

        logger.info("Training recommender system..")
        np.set_printoptions(edgeitems=1,
                            infstr='inf',
                            linewidth=275,
                            precision=3,
                            suppress=False,
                            formatter=None)

        # once the matrix is filled, look for missing columns:
        col_size = ts_matrix.shape[1]
        mapped_columns = []
        for col_idx in range(col_size):
            if (ts_matrix[:, col_idx] < 0).all():
                pass
            else:
                mapped_columns.append(col_idx)

        working_matrix = ts_matrix[:, mapped_columns]

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
                dist = -pdist(np.vstack((mat_norm[valid_rows == True, c],
                                         mat_norm[valid_rows == True, cc])), 'cosine') + 1
                item_simil_mat[c, cc] = dist.item(0)

        # retain only max values in similarity matrix
        n_stencil = int(round(item_simil_mat.shape[0] * 3. / 4.))
        item_simil_mat_stencil = np.copy(item_simil_mat)
        for row in item_simil_mat_stencil:
            ind = np.argpartition(row, -n_stencil)[-n_stencil:]
            not_max_mask = np.ones(row.shape, dtype=bool)
            not_max_mask[ind] = False
            row[not_max_mask] = 0.0

        item_prediction_norm = mat_norm.dot(item_simil_mat_stencil) / \
                               (np.array([np.abs(item_simil_mat_stencil).sum(axis=1)]) + 1.e-20)
        filled_matrix = item_prediction_norm * item_max_train

        # re-apply filled matrix to jobs
        wl_dest.matrix_to_jobs(filled_matrix, priority, mapped_columns, n_bins)

