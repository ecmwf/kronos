import os
import json


class Config(object):
    """
    A storage place for global configuration, including parsing of input JSON, and error checking
    """
    # DIR_LOG_OUTPUT = "/perm/ma/maab/temp/test_logs"
    # DIR_DARSHAN_LIB = "/usr/local/apps/darshan/2.3.1-ecm1.1/lib64"
    # DIR_OPENMPI_SRC = "/usr/lib64/mpi/gcc/openmpi/include"
    # DIR_OPENMPI_LIB = "/usr/lib64/mpi/gcc/openmpi/lib64"
    # DIR_BOOST_SRC = "/usr/local/apps/boost/1.55.0/include"
    # DIR_BOOST_LIB = "/usr/local/apps/boost/1.55.0/lib"
    # FNAME_CSV_OUT = "raw_data.csv"

    # ------- real workload ----------
    REALWORKLOAD_TOTAL_METRICS_NBINS = 10

    # --------- WL corrector ---------
    WORKLOADCORRECTOR_EPS = 1.0e-5  # tolerance for ANN estimates
    WORKLOADCORRECTOR_NTIME = 100
    WORKLOADCORRECTOR_NFREQ = 3
    WORKLOADCORRECTOR_ANN_INPUT_NAMES = ["ncpus", "runtime"]
    WORKLOADCORRECTOR_ANN_NNEURONS = 20
    WORKLOADCORRECTOR_ANN_LEARNINGRATE = 0.001
    WORKLOADCORRECTOR_ANN_MOMENTUM = 0.002
    WORKLOADCORRECTOR_ANN_WEIGHTDECAY = 0.00001
    WORKLOADCORRECTOR_ANN_SPLITRATIO = 0.9
    WORKLOADCORRECTOR_ANN_EPOCHS = 10

    WORKLOADCORRECTOR_JOBS_NBINS = 10

    # IOWS Model

    IOWSMODEL_TOTAL_METRICS_NBINS = 10
    IOWSMODEL_KMEANS_MAXITER = 8000
    IOWSMODEL_KMEANS_KMEANS_RSEED = 170
    IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH = 0.2
    IOWSMODEL_SUPPORTED_SYNTH_APPS = ['cpu', 'file-read', 'file-write', 'mpi']

    def __init__(self, config_dict=None, config_path=None):

        assert config_dict is None or config_path is None

        if config_path is not None:
            with open(config_path, 'r') as f:
                config_dict = json.load(f)

        config_dict = config_dict if config_dict is not None else {}

        # And initialise using some default values

        self.dir_output = config_dict.get('dir_output', os.path.join(os.getcwd(), 'output'))
        self.dir_input = config_dict.get('dir_output', os.path.join(os.getcwd(), 'input'))
        if not os.path.exists(self.dir_output):
            print "Creating output directory: {}".format(self.dir_output)
            os.makedirs(self.dir_output)

        # Sources of profiling data

        self.profile_sources = config_dict.get('profile_sources', [])

