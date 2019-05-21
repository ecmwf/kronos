from kronos_modeller.strategy_base import StrategyBase


class EditingStrategy(StrategyBase):

    """
    Minimal interface for workload editing strategy classes
    """

    required_config_fields = [
        "type",
    ]
