
import logging

from kronos_modeller.job_filling.match_keyword import StrategyMatchKeyword
from kronos_modeller.job_filling.user_defaults import StrategyUserDefaults
from kronos_modeller.job_filling.recommender_sys import StrategyRecommender

logger = logging.getLogger(__name__)


job_filling_types = {
    "user_defaults": StrategyUserDefaults,
    "match_by_keyword": StrategyMatchKeyword,
    "recommender_system": StrategyRecommender
}
