import os


class Config(object):
    """temporary quick&dirty solution to store config keys (TODO)"""

    # --------- directories ----------
    DIR_OUTPUT = os.path.join(os.getcwd(), "/home/ma/maab/workspace/iows/output")
    DIR_INPUT = os.path.join(os.getcwd(), "/home/ma/maab/workspace/iows/input")
    if not os.path.exists(DIR_OUTPUT):
        os.mkdir(DIR_OUTPUT)

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

    # TODO: this needs to be read from time-traces
    # # time signals = [name, type, "kernel type"]
    # WORKLOADCORRECTOR_LIST_TIME_NAMES = [('cpu_L1_cache_misses',  'int', 'cpu'),
    #                                      ('cpu_L2_cache_misses',  'int', 'cpu'),
    #                                      ('cpu_L3_cache_misses',  'int', 'cpu'),
    #                                      ('cpu_flops',            'int', 'cpu'),
    #                                      ('cpu_n_instructions',   'int', 'cpu'),
    #                                      ('cpu_n_cycles',         'int', 'cpu'),
    #                                      ('io_n_reads',           'int', 'file-read'),
    #                                      ('io_bytes_read',        'int', 'file-read'),
    #                                      ('io_n_writes',          'int', 'file-write'),
    #                                      ('io_bytes_writes',      'int', 'file-write'),
    #                                      ('memory_percent',         'int', 'memory'),
    #                                      ('mpi_n_p2p',            'int', 'mpi'),
    #                                      ('mpi_bytes_p2p',        'int', 'mpi'),
    #                                      ('mpi_n_collective',     'int', 'mpi'),
    #                                      ('mpi_bytes_collective', 'int', 'mpi')]

    # time signals = [name, type, "kernel type", "re-sampling method"] (reduced list  compatible with Allinea metrics)
    WORKLOADCORRECTOR_LIST_TIME_NAMES = [('flops',               'double', 'cpu',         'sum'),
                                        ('kb_read',              'double', 'file-read',   'sum'),
                                        ('kb_write',             'double', 'file-write',  'sum'),
                                        ('n_pairwise',           'double', 'mpi',         'sum'),
                                        ('kb_pairwise',          'double', 'mpi',         'sum'),
                                        ('n_collective',         'double', 'mpi',         'sum'),
                                        ('kb_collective',        'double', 'mpi',         'sum')]


    # IOWS Model
    IOWSMODEL_TOTAL_METRICS_NBINS = 10
    IOWSMODEL_KMEANS_MAXITER = 8000
    IOWSMODEL_KMEANS_KMEANS_RSEED = 170
    IOWSMODEL_JOB_IMPACT_INDEX_REL_TSH = 0.2
