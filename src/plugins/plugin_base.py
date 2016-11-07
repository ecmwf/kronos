
from postprocess import statistics


class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config

    def ingest_data(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call clustering.factory")

    def postprocess(self):
        """
        Do some post-processing of the synthetic apps (modelled and run)
        :return:
        """

        if self.config.post_process["post_process_scope"] == "input":
            stats = statistics.Statistics(self.config)
            stats.calculate_sa_metrics()
            stats.print_sa_stats()
            stats.plot_sa_stats()

        if self.config.post_process["post_process_scope"] == "output":
            stats = statistics.Statistics(self.config)
            stats.calculate_run_metrics()





