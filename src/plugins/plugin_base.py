from kronos_tools.print_colour import print_colour
import logreader
from logreader.dataset import IngestedDataSet
from postprocess import statistics


class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config
        self.job_datasets = None

    def ingest_data(self):

        self.job_datasets = []

        print_colour("green", "ingesting data..")

        # ingest datasets from pickled files if any:
        if self.config.loaded_datasets:
            for ingest_type, ingest_path in self.config.loaded_datasets:
                print "ingesting {}".format(ingest_path)
                self.job_datasets.append(IngestedDataSet.from_pickled(ingest_path))

        # ingest from logs if any:
        if self.config.loaded_datasets:
            for ingest_type, ingest_path in self.config.profile_sources:
                self.job_datasets.append(logreader.ingest_data(ingest_type, ingest_path, self.config))

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")

    def postprocess(self, postprocess_flag):
        """
        Do some post-processing of the synthetic apps (modelled and run)
        :return:
        """

        if postprocess_flag == "input":
            stats = statistics.Statistics(self.config)
            stats.read_sa_metrics_from_jsons()
            stats.print_sa_stats()
            stats.plot_sa_stats()

        if postprocess_flag == "output":
            stats = statistics.Statistics(self.config)
            stats.calculate_run_metrics()





