import logging

from kronos_modeller.workload_modelling.job_generation.job_generator_spawn import JobGeneratorSpawn
from kronos_modeller.workload_modelling.job_generation.job_generator_spawn_rand import JobGeneratorSpawnRand

logger = logging.getLogger(__name__)


strategy_factory = {
    "spawn": JobGeneratorSpawn,
    "spawn_random": JobGeneratorSpawnRand,
}


# import logging
#
# from kronos_modeller.workload_modelling.modelling_strategy import WorkloadModellingStrategy
#
# logger = logging.getLogger(__name__)
#
#
# generation_types = {
#     "spawn": WorkloadModellingStrategy
# }
