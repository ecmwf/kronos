from datetime import datetime

import os
import json
import glob
import numpy as np

from jobs import IngestedJob, ModelJob
from time_signal import TimeSignal
from exceptions_iows import ConfigurationError
from logreader.dataset import IngestedDataSet
import time_signal


def read_allinea_log(filename, jobs_n_bins=None):
    """ Collect info from Allinea logs """

    # The time signal map has a number of options for each element in the profile:
    #
    # 'name':     What is the name of this signal mapped into IOWS-land (i.e. mapping onto time_signal.signal_types)
    # 'is_rate':  True if the data is recorded as x-per-second rates, rather than accumulatable values.
    #             (default False)
    # 'per_task': Is the value presented per-task, or global. If per-task it needs to be multiplied up.
    #             (default False)

    allinea_time_signal_map = {
        'cpu_time_percentage':  {'name': 'flops'},
        'lustre_bytes_read':    {'name': 'kb_read',       'is_rate': True, 'scale_factor': 1./1024.},
        'lustre_bytes_written': {'name': 'kb_write',      'is_rate': True, 'scale_factor': 1./1024.},
        'mpi_p2p':              {'name': 'n_pairwise'},
        'mpi_p2p_bytes':        {'name': 'kb_pairwise',                     'scale_factor': 1./1024.},
        'mpi_collect':          {'name': 'n_collective'},
        'mpi_collect_bytes':    {'name': 'kb_collective',                   'scale_factor': 1./1024.}
    }

    # A quick sanity check
    for value in allinea_time_signal_map.values():
        assert value['name'] in time_signal.signal_types

    with open(filename) as json_file:
        json_data = json.load(json_file)

    # fill in the workload structure
    i_job = IngestedJob()

    time_start = json_data['profile']['timestamp']
    runtime = float(json_data['profile']['runtime_ms']) / 1000.
    time_start_epoch = (datetime.strptime(time_start, "%a %b %d %H:%M:%S %Y") -
                        datetime(1970, 1, 1)).total_seconds()

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
    i_job.nnodes = json_data['profile']["nodes"]
    i_job.ncpus = json_data['profile']['targetProcs']

    # average memory used is taken from sample average of "node_mem_percent"
    mem_val_bk = json_data['profile']['samples']['node_mem_percent']
    mem_val = [v[2] for v in mem_val_bk]  # values inside the blocks are: min, max, mean, var
    mem_val_mean = sum(mem_val) / float(len(mem_val))/100.
    mem_node_kb = json_data['profile']["memory_per_node"][2] / 1024.
    i_job.memory_kb = mem_node_kb * mem_val_mean

    i_job.cpu_percent = 0

    i_job.jobname = filename
    i_job.user = "job-profiler"
    i_job.group = ""
    i_job.queue_type = ""

    # # times relative to start of log
    # profiler jobs are considered as if they were started at T0
    # TODO: find more sensible solution to that..
    i_job.time_start_0 = 0.0

    # Obtain the timestamps for the (end of) each sampling window, converted into seconds.
    sample_times = np.array(json_data['profile']['sample_times']) / 1000.

    for ts_name_allinea, ts_config in allinea_time_signal_map.iteritems():

        scale_factor = ts_config.get('scale_factor', 1.0)

        # The Allinea time-series data is a sequence of tuples of the form: (min, max, mean, variance)
        # Extract the mean value for each sampling interval.
        y_vals = np.array([v[2] * scale_factor for v in json_data['profile']['samples'][ts_name_allinea]])

        # If the data is recorded as a rate (a per-second value), then adjust it to record absolute data volumes
        # per time interval.
        if ts_config.get('is_rate', False):
            y_vals = np.array([v * (sample_times[i] - (sample_times[i-1] if i > 0 else 0)) for i, v in enumerate(y_vals)])

        if ts_config.get('per_task', False):
            y_vals *= i_job.ncpus

        ts = TimeSignal.from_values(ts_config['name'], sample_times, y_vals)
        if jobs_n_bins is not None:
            ts.digitize(jobs_n_bins)
        i_job.append_time_signal(ts)

    return i_job


def read_allinea_logs(log_dir, jobs_n_bins=None, list_json_files=None):

    """
    Collect info from Allinea logs
    """
  # pick up the list of json files to process
    if list_json_files is None:
        json_files = glob.glob(os.path.join(os.path.realpath(log_dir), "*.json"))
        print "reading json files..."
        json_files.sort()
    else:
        json_files = list_json_files
        print "reading json files..."
        json_files.sort()

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


def ingest_allinea_profiles(path, jobs_n_bins=None, list_json_files=None):
    """
    Does what it says on the tin.
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest Allinea profiles does not exist: {}".format(path))

    if not list_json_files:
        if os.path.isdir(path):
            jobs = read_allinea_logs(path, jobs_n_bins)
        else:
            jobs = [read_allinea_log(path, jobs_n_bins)]
    else:
        jobs = read_allinea_logs(path, jobs_n_bins, list_json_files)

    return AllineaDataSet(jobs)


