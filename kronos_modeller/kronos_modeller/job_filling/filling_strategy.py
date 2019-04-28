from kronos_modeller.strategy_base import StrategyBase


class FillingStrategy(StrategyBase):

    required_config_fields = [
        "type",
        "priority"
    ]
