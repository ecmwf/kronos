from exceptions_iows import ConfigurationError


class ClusteringBase(object):
    """
    Base class, defining structure for data_analysis algorithms
    """
    def __init__(self, config):
        assert isinstance(config, dict)
        self.config = config
        self.clusters = None
        self.labels = None
        self.num_timesignal_bins = None

        self.check_config()

    def check_config(self):
        """
        Update the default values using the supplied configuration dict
        :return:
        """

        # check that all the required fields are set
        for req_item in self.required_config_fields:
            if req_item not in self.config.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))

        # check that configuration keys are correct
        for k, v in self.config.items():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected ClusteringDBSCAN keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def cluster_jobs(self, timesignal_matrix):
        """
        Apply data_analysis to passed model jobs
        :param input_jobs: List of input jobs
        :return:
        """

        (self.clusters, self.labels) = self.apply_clustering(timesignal_matrix)

    def apply_clustering(self, input_jobs):
        """
        Specific data_analysis method that applies to the unrolled jobs metrics
        :param input_jobs: input model jobs
        :return:
        """
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")