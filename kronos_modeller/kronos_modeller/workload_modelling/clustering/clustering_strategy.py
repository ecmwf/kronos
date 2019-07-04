from kronos_modeller.strategy_base import StrategyBase


class ClusteringStrategy(StrategyBase):

    required_config_fields = [
        "type",
        "apply_to",
        'rseed',
        "num_timesignal_bins",
        'max_num_clusters'
    ]

    def __init__(self, workloads):

        super(ClusteringStrategy, self).__init__(workloads)

        # job clusters found by the algorithm
        # (a classification strategy, when applied only finds the clusters.
        # the strategy user then needs to get the clusters for later use.
        self.clusters = []

    def get_clusters(self):

        """
        Explicitly return the job clusters found
        :return:
        """

        return self.clusters
