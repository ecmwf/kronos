import data_analysis
from config.config import Config


class ClusteringBase(object):
    """
    Base class, defining structure for data_analysis algorithms
    """
    def __init__(self, config):
        assert isinstance(config, dict)
        self.config = config
        self.clusters = None
        self.labels = None

        self.check_config()

    def check_config(self):
        """
        checks and sets default config options
        """
        pass

    def cluster_jobs(self, input_jobs):
        """
        Apply data_analysis to passed model jobs
        :param input_jobs: List of input jobs
        :return:
        """
        timesignal_matrix = data_analysis.jobs_to_matrix(input_jobs)
        (self.clusters, self.labels) = self.apply_clustering(timesignal_matrix)

    def apply_clustering(self, input_jobs):
        """
        Specific data_analysis method that applies to the unrolled jobs metrics
        :param input_jobs: input model jobs
        :return:
        """
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")