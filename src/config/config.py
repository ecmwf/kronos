import os
import json

from exceptions_iows import ConfigurationError
import time_signal


class Config(object):
    """
    A storage place for global configuration, including parsing of input JSON, and error checking
    """

    # name of the plugin to load up
    plugin = None

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

    IOWSMODEL_TOTAL_METRICS_NBINS = 1
    IOWSMODEL_KMEANS_MAXITER = 8000
    IOWSMODEL_KMEANS_KMEANS_RSEED = 170
    IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH = 0.2
    IOWSMODEL_SUPPORTED_SYNTH_APPS = ['cpu', 'file-read', 'file-write', 'mpi']

    # model run options (each runner has different options)
    # TODO: addd consistency checks..
    runner = None
    controls = None
    post_process = None

    metrics_names = time_signal.signal_types.keys()

    # unit scaling factors
    unit_sc_dict = {}
    for m in metrics_names:
        unit_sc_dict[m] = 1.0
    # ---------------------------------------------------------



    # Directory choices

    # dir_output = os.path.join(os.getcwd(), 'output')
    # dir_input = os.path.join(os.getcwd(), 'input')

    dir_output = None
    dir_input = None

    # Sources of profiling data

    profile_sources = []

    ingestion = {}

    # Modelling and data_analysis details

    model_clustering = "none"
    model_clustering_algorithm = None
    model_scaling_factor = 1.0

    # hardware

    CPU_FREQUENCY = 2.3e9  # Hz

    # Debugging info

    verbose = False

    def __init__(self, config_dict=None, config_path=None):

        assert config_dict is None or config_path is None

        # If specified, load a config from the given JSON file. Custom modification to the JSON spec
        # permits lines to be commented out using a '#' character for ease of testing
        if config_path is not None:
            with open(config_path, 'r') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    if '#' in line:
                        lines[i] = line[:line.index('#')]

                config_dict = json.loads(''.join(lines))

        config_dict = config_dict if config_dict is not None else {}

        # Update the default values using the supplied configuration dict
        if not isinstance(config_dict, dict):
            raise ConfigurationError("no keys found in configuration file")

        for k, v in config_dict.iteritems():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

        # And any necessary actions
        if not self.dir_input:
            raise ConfigurationError("input folder not set")

        if not self.dir_output:
            raise ConfigurationError("output folder not set")

        # if input or output folders do not exist, an error is raised
        if not os.path.exists(self.dir_input):
            raise ConfigurationError("input folder {} - does not exist!".format(self.dir_input))

        if not os.path.exists(self.dir_output):
            raise ConfigurationError("output folder {} - does not exist!".format(self.dir_output))
