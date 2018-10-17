# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from kronos.core.exceptions_iows import ConfigurationError


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
                raise ConfigurationError("{}: Unexpected Clustering keyword provided - {}:{}".format(self.__class__.__name__, k, v))
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