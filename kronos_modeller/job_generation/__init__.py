from kronos.kronos_modeller.job_generation.strategy_spawn_rand import StrategySpawnRand

from kronos_modeller.job_generation.strategy_spawn import StrategySpawn

strategy_factory = {
    "spawn": StrategySpawn,
    "spawn_random": StrategySpawnRand,
}