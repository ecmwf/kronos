import logging
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