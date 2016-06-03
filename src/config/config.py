import os


class Config(object):

    """temporary quick&dirty solution to store config keys (TODO)"""

    # --------- directories ----------
    DIR_OUTPUT = os.path.join(os.getcwd(), "output")
    DIR_INPUT = os.path.join(os.getcwd(), "input")
    if not os.path.exists(DIR_OUTPUT):
        os.mkdir(DIR_OUTPUT)

    DIR_LOG_OUTPUT = "/perm/ma/maab/temp/test_logs"
    DIR_DARSHAN_LIB = "/usr/local/apps/darshan/2.3.1-ecm1.1/lib64"
    DIR_OPENMPI_SRC = "/usr/lib64/mpi/gcc/openmpi/include"
    DIR_OPENMPI_LIB = "/usr/lib64/mpi/gcc/openmpi/lib64"
    DIR_BOOST_SRC = "/usr/local/apps/boost/1.55.0/include"
    DIR_BOOST_LIB = "/usr/local/apps/boost/1.55.0/lib"

    FNAME_CSV_OUT = "raw_data.csv"

    # ---------- dummy workload -------------
    NAME_DUMMY_MASTER = "dummy_master.cpp"
    DUMMY_WORKLOAD_NJOBS = 10
    DUMMY_WORKLOAD_NPROCS_TOTAL = 8
    DUMMY_WORKLOAD_TIME_SCHEDULE_TOTAL = 10
    DUMMY_WORKLOAD_TIME_APPLICATION_MAX = 3

    # ------- real workload ----------
    REALWORKLOAD_TOTAL_METRICS_NBINS = 10

    # --------- WL corrector ---------
    WORKLOADCORRECTOR_NTIME = 100
    WORKLOADCORRECTOR_NFREQ = 3
    WORKLOADCORRECTOR_ANN_INPUT_NAMES = ["ncpus", "runtime", "time_in_queue"]
    WORKLOADCORRECTOR_ANN_NNEURONS = 20
    WORKLOADCORRECTOR_ANN_LEARNINGRATE = 0.001
    WORKLOADCORRECTOR_ANN_MOMENTUM = 0.002
    WORKLOADCORRECTOR_ANN_WEIGHTDECAY = 0.00001
    WORKLOADCORRECTOR_ANN_SPLITRATIO = 0.9
    WORKLOADCORRECTOR_ANN_EPOCHS = 200

    WORKLOADCORRECTOR_JOBS_NBINS = 10

    #(TODO: this needs to be read from time-traces
    WORKLOADCORRECTOR_LIST_TIME_NAMES = [('flops',      'int',   'CPU'),
                                         ('n_read',     'int',   'IO'),
                                         ('n_write',    'int',   'IO'),
                                         ('kb_read',    'float', 'IO'),
                                         ('kb_write',   'float', 'IO'),
                                         ('n_p2p',      'int',   'MPI'),
                                         ('kb_p2p',     'float', 'MPI'),
                                         ('n_collect',  'int',   'MPI'),
                                         ('kb_collect', 'float', 'MPI')]

    # ---------- IOWS Model ----------
    IOWSMODEL_TOTAL_METRICS_NBINS = 10
    IOWSMODEL_KMEANS_MAXITER = 8000
    IOWSMODEL_KMEANS_KMEANS_RSEED = 170
    IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH = 0.2
