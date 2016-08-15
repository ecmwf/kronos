import os
import json
import time

from exceptions_iows import ConfigurationError
import time_signal

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

    IOWSMODEL_TOTAL_METRICS_NBINS = 1
    IOWSMODEL_KMEANS_MAXITER = 8000
    IOWSMODEL_KMEANS_KMEANS_RSEED = 170
    IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH = 0.2
    IOWSMODEL_SUPPORTED_SYNTH_APPS = ['cpu', 'file-read', 'file-write', 'mpi']


    # IOWS model tuning (feedback-loop)

    FL_RUN_TAG = "run_" + str(time.time())

    # dir for iows and kronos
    FL_IOWS_DIR = "/var/tmp/maab/iows"
    FL_IOWS_DIR_INPUT = FL_IOWS_DIR + "/input"
    FL_IOWS_DIR_OUTPUT = FL_IOWS_DIR + "/output"
    FL_IOWS_DIR_BACKUP = FL_IOWS_DIR_INPUT + "/" + FL_RUN_TAG
    FL_KRONOS_RUN_DIR = "/scratch/ma/maab/kronos_run"
    FL_USER_HOST = "maab@ccb"
    FL_n_iterations = 1
    FL_LOG_FILE = FL_IOWS_DIR_BACKUP + '/' + FL_RUN_TAG + '_log.txt'
    FL_updatable_metrics = {'kb_collective': 1,
                         'n_collective': 1,
                         'n_pairwise': 1,
                         'kb_write': 1,
                         'kb_read': 1,
                         'flops': 0,
                         'kb_pairwise': 1,
                         }

    metrics_names = time_signal.signal_types.keys()

    # unit scaling factors
    unit_sc_dict = {}
    for m in metrics_names:
        unit_sc_dict[m] = 1.0
    # ---------------------------------------------------------



    # Directory choices

    dir_output = os.path.join(os.getcwd(), 'output')
    dir_input = os.path.join(os.getcwd(), 'input')

    # Sources of profiling data

    profile_sources = [('allinea', '/var/tmp/maab/iows/input')]

    ingestion = {}

    # Modelling and clustering details

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

        for k, v in config_dict.iteritems():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

        # And any necessary actions

        if not os.path.exists(self.dir_output):
            print "Creating output directory: {}".format(self.dir_output)
            os.makedirs(self.dir_output)


