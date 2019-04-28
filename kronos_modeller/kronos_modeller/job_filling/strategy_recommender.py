import logging

from kronos_modeller.job_filling import recommender
from kronos_modeller.job_filling.strategy_base import StrategyBase

logger = logging.getLogger(__name__)


class StrategyRecommender(StrategyBase):

    """
    Apply recommender system to jobs of a
    certain workload type
    """

    required_config_fields = StrategyBase.required_config_fields + \
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

        # uses a recommender model
        recomm_sys = recommender.Recommender(ts_matrix, n_bins)
        filled_matrix, mapped_columns = recomm_sys.apply()

        # re-apply filled matrix to jobs
        wl_dest.matrix_to_jobs(filled_matrix, priority, mapped_columns, n_bins)
