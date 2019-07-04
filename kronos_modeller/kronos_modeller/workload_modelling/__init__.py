import logging
from kronos_modeller.workload_modelling.cluster_spawn import ClusterSpawnStrategy

logger = logging.getLogger(__name__)


workload_modelling_types = {
    "cluster_and_spawn": ClusterSpawnStrategy
}