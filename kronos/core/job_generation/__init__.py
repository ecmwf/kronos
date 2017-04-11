from kronos.core.job_generation.strategy_spawn import StrategySpawn
from kronos.core.job_generation.strategy_spawn_rand import StrategySpawnRand

strategy_factory = {
    "spawn": StrategySpawn,
    "spawn_random": StrategySpawnRand,
}