from datetime import datetime
import os
import json
import glob

from jobs import IngestedJob, ModelJob
from time_signal import TimeSignal
from exceptions_iows import ConfigurationError
from logreader.dataset import IngestedDataSet


def read_allinea_log(filename, jobs_n_bins=None):
    """ Collect info from Allinea logs """

    desired2allinea = [('flops',            'instr_fp',             'cpu',          'sum'),
                       ('kb_read',          'lustre_bytes_read',    'file-read',    'sum'),
                       ('kb_write',         'lustre_bytes_written', 'file-write',   'sum'),
                       ('n_pairwise',       'mpi_p2p',              'mpi',          'sum'),
                       ('kb_pairwise',      'mpi_p2p_bytes',        'mpi',          'sum'),
                       ('n_collective',     'mpi_collect',          'mpi',          'sum'),
                       ('kb_collective',    'mpi_collect_bytes',    'mpi',          'sum')]

    with open(filename) as json_file:
        json_data = json.load(json_file)

    # fill in the workload structure
    i_job = IngestedJob()

    time_start = json_data['profile']['timestamp']
    runtime = float(json_data['profile']['runtime_ms']) / 1000.
    time_start_epoch = (datetime.strptime(time_start, "%a %b %d %H:%M:%S %Y") -
                        datetime(1970, 1, 1)).total_seconds()

    # this job might not necessarily been queued
    i_job.time_created = time_start_epoch - 3
    i_job.time_queued = time_start_epoch - 2
    i_job.time_eligible = time_start_epoch - 1
    i_job.time_start = time_start_epoch
    i_job.runtime = runtime

    i_job.time_end = time_start_epoch + runtime
    i_job.time_in_queue = i_job.time_start - i_job.time_queued

    # Threads are not considered for now..
    i_job.ncpus = (json_data['profile']["nodes"] * json_data['profile']["num_physical_cores_per_node"][2])
    i_job.nnodes = json_data['profile']["nodes"]

    # average memory used is taken from sample average of "node_mem_percent"
    mem_val_bk = json_data['profile']['samples']['node_mem_percent']
    mem_val = [v[2] for v in mem_val_bk]  # values inside the blocks are: min, max, mean, var
    mem_val_mean = sum(mem_val) / float(len(mem_val))/100.
    mem_node_kb = json_data['profile']["memory_per_node"][2] / 1024.
    i_job.memory_kb = mem_node_kb * mem_val_mean

    i_job.cpu_percent = 0

    i_job.jobname = os.path.basename(filename)
    i_job.user = "job-profiler"
    i_job.group = ""
    i_job.queue_type = ""

    # # times relative to start of log
    # profiler jobs are considered as if they were started at T0
    # TODO: find more sensible solution to that..
    i_job.time_start_0 = 0.0

    sample_times = json_data['profile']['sample_times']

    # convert time in seconds
    sample_times = [t / 1000. for t in sample_times]

    # ts_names_std_list = [row[1] for row in desired2allinea]
    # for ts_name_allinea in json_data['profile']['samples'].keys():
    for ts_name_row in desired2allinea:

        # convert name into corresponding desired metrics name..
        # name_ker_ts_std = [(i_name[0], i_name[2], i_name[3]) for i_name in desired2allinea if i_name[1] == ts_name_allinea]

        ts_name_std = ts_name_row[0]
        ts_name_allinea = ts_name_row[1]
        ker_type = ts_name_row[2]
        digit_type = ts_name_row[3]

        # print ts_name_allinea, ts_name_allinea, ker_type, digit_type

        # if not (name_ker_ts_std == []):
        y_val_bk = json_data['profile']['samples'][ts_name_allinea]
        y_val = [v[2] for v in y_val_bk]  # values inside the blocks are: min, max, mean, var

        # Correct the values for "kb_pairwise" and "kb_collective" (as they are recorded as PER SECOND)
        if (ts_name_std=="kb_pairwise") or (ts_name_std=="kb_collective"):
            t_ext = [0] + sample_times
            y_val_ext = [0] + y_val
            y_val = [y_val_ext[tt-1]*(t_ext[tt]-t_ext[tt-1]) for tt in range(1, len(sample_times)+1)]

        ts = TimeSignal()
        ts.create_ts_from_values(ts_name_std, "float", ker_type, sample_times, y_val)
        if jobs_n_bins is not None:
            ts.digitize(jobs_n_bins, digit_type)
        i_job.append_time_signal(ts)

        # -- translate allinea metrics into desired metrics
        # 'mpi_call_time',
        # 'nvidia_memory_sys_usage',
        # 'mpi_collect',
        # 'mpi_p2p_bytes',
        # 'rchar_total',
        # 'mpi_recv',
        # 'instr_branch',
        # 'wchar_total',
        # 'instr_scalar_int',
        # 'instr_vector_fp',
        # 'instr_vector_int',
        # 'lustre_bytes_written',
        # 'nvidia_memory_used',
        # 'rapl_energy',
        # 'node_mem_percent',
        # 'mpi_sent',
        # 'involuntary_context_switches',
        # 'system_time_percentage',
        # 'instr_other',
        # 'bytes_read',
        # 'lustre_bytes_read',
        # 'instr_fp',
        # 'system_energy',
        # 'instr_scalar_fp',
        # 'nvidia_temp',
        # 'system_power',
        # 'instr_implicit_mem',
        # 'nvidia_memory_used_percent',
        # 'user_time_percentage',
        # 'rss',

    return i_job


def read_allinea_logs(log_dir, jobs_n_bins=None):
    """
    Collect info from Allinea logs
    """
    json_files = glob.glob(os.path.join(os.path.realpath(log_dir), "*.json"))

    print "reading json files..."

    return [ read_allinea_log(filename, jobs_n_bins) for filename in json_files ]


class AllineaDataSet(IngestedDataSet):

    def __init__(self, joblist, *args, **kwargs):
        super(AllineaDataSet, self).__init__(joblist, *args, **kwargs)

        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        self.global_start_time = min((j.time_created for j in self.joblist))

    def model_jobs(self):
        for job in self.joblist:
            assert isinstance(job, IngestedJob)

            yield ModelJob(
                time_start=job.time_created-self.global_start_time,
                duration=job.time_end-job.time_start,
                ncpus=job.ncpus,
                nnodes=job.nnodes,
                time_series=job.timesignals
            )


def ingest_allinea_profiles(path, jobs_n_bins=None):
    """
    Does what it says on the tin.
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest Allinea profiles does not exist: {}".format(path))

    if os.path.isdir(path):
        jobs = read_allinea_logs(path, jobs_n_bins)
    else:
        jobs = [read_allinea_log(path, jobs_n_bins)]

    return AllineaDataSet(jobs)


