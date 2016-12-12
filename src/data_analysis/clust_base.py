
import numpy as np
from kronos_tools.print_colour import print_colour


class ClusteringBase(object):
    """
    Base class, defining structure for data_analysis algorithms
    """
    def __init__(self, config):
        assert isinstance(config, dict)
        self.config = config
        self.clusters = None
        self.labels = None
        self.n_ts_bins = None
        self.metrics_only = False

        self.check_config()

    def check_config(self):
        """
        checks and sets default config options
        """
        pass

    def cluster_jobs(self, timesignal_matrix):
        """
        Apply data_analysis to passed model jobs
        :param input_jobs: List of input jobs
        :return:
        """

        try:
            matrix_rank = np.linalg.matrix_rank(timesignal_matrix)
            matrix_col_size = timesignal_matrix.shape[1]
        except ValueError:
            matrix_col_size = timesignal_matrix.shape[1]
            matrix_rank = matrix_col_size

        # if rank <= n columns just use the first jobs as clusters..
        if matrix_rank <= matrix_col_size:

            print_colour("orange", "clustering has low rank matrix => it will fail..")
            print "matrix_rank", timesignal_matrix[:matrix_col_size, :matrix_col_size]

            if self.config['ok_if_low_rank']:
                print_colour("green", "==> the model will be built directly from {} jobs".format(timesignal_matrix.shape[1]))
                self.clusters = timesignal_matrix[:matrix_col_size]
                self.labels = None

            else:
                raise ValueError("Job matrix has low rank={}, clustering FAILED!".format(matrix_rank))
        else:

            (self.clusters, self.labels) = self.apply_clustering(timesignal_matrix)

    def apply_clustering(self, input_jobs):
        """
        Specific data_analysis method that applies to the unrolled jobs metrics
        :param input_jobs: input model jobs
        :return:
        """
        raise NotImplementedError("Must use derived class. Call data_analysis.factory")