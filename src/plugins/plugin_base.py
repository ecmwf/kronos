import os

from kpf_handler import KPFFileHandler
from kronos_tools.print_colour import print_colour


class PluginBase(object):

    """
    Base class, defining structure for plugins
    """

    def __init__(self, config):
        self.config = config
        self.job_datasets = None
        self.workloads = None

    def ingest_data(self):

        self.job_datasets = []

        print_colour("green", "ingesting data..")

        # # ingest datasets from pickled files if any:
        # if self.config.loaded_datasets:
        #     for ingest_type, ingest_path in self.config.loaded_datasets:
        #         print "ingesting {}".format(ingest_path)
        #         self.job_datasets.append(IngestedDataSet.from_pickled(ingest_path))
        #
        # # ingest from logs if any:
        # if self.config.loaded_datasets:
        #     for ingest_type, ingest_path in self.config.profile_sources:
        #         self.job_datasets.append(logreader.ingest_data(ingest_type, ingest_path, self.config))

        # load job data sets from a kpf file..
        self.workloads = KPFFileHandler().load_kpf(os.path.join(self.config.dir_input, self.config.kpf_file))

    def generate_model(self):
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")

    def run(self):
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")

    def postprocess(self, postprocess_flag):
        """
        Do some post-processing of the synthetic apps (modelled and run)
        :return:
        """

        # if postprocess_flag == "input":
        #     stats = statistics.Statistics(self.config)
        #     stats.read_ksf_data()
        #     stats.print_sa_stats()
        #     # stats.plot_sa_stats()
        #
        # if postprocess_flag == "output":
        #     stats = statistics.Statistics(self.config)
        #     stats.plot_from_logfile(self.config.dir_output)
        #     # stats.calculate_run_metrics()





