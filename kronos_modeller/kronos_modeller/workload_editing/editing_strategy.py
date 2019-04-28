from kronos_modeller.strategy_base import StrategyBase


class EditingStrategy(StrategyBase):

    required_config_fields = [
        "type",
        "priority"
    ]
