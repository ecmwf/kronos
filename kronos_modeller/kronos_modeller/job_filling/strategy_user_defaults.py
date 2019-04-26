from kronos_modeller.job_filling.strategy_base import StrategyBase


class StrategyUserDefaults(StrategyBase):

    """
    Apply user defined defaults to jobs of a
    certain workload type
    """

    required_config_fields = StrategyBase.required_config_fields + \
        [
            "apply_to",
            "metrics"
        ]

    def _apply(self, config, user_functions):

        print "applying defaults"

        # apply function to all the specified workloads
        for wl in self.workloads:
            if wl.tag in config['apply_to']:
                wl.apply_default_metrics(config, user_functions)
