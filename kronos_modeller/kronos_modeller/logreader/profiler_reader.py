# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from datetime import datetime
import json
import logging
import pathlib
import os
import sys

import numpy as np

from kronos_executor.definitions import signal_types
from kronos_modeller.kronos_exceptions import ConfigurationError
from kronos_modeller.jobs import IngestedJob, ModelJob
from kronos_modeller.logreader.dataset import IngestedDataSet
from kronos_modeller.time_signal.time_signal import TimeSignal

logger = logging.getLogger(__name__)


allinea_signal_priorities = {
    'flops': 10,
    'kb_read': 10,
    'kb_write': 10,
    'n_read': 10,
    'n_write': 10,
    'n_pairwise': 10,
    'kb_pairwise': 10,
    'n_collective': 10,
    'kb_collective': 10,
}


def read_allinea_log(filename, jobs_n_bins=None, cfg=None):
    """ Collect info from Allinea logs """

    # The time signal map has a number of options for each element in the profile:
    #
    # 'name':     What is the name of this signal mapped into Kronos-land (i.e. mapping onto time_signal.signal_types)
    # 'is_rate':  True if the data is recorded as x-per-second rates, rather than accumulatable values.
    #             (default False)
    # 'per_task': Is the value presented per-task, or global. If per-task it needs to be multiplied up.
    #             (default False)

    logger.info(
                 "NOTE: FLOPS not available for allinea Dataset: it will be estimated from %CPU and clock rate")

    # check of the clock_rate is passed in the config
    if not cfg:
        logger.info(
                     "WARNING: clock rate not provided! arbitrarily set to 2.5GHz")
        clock_rate = 2.5e9
    else:
        if cfg.get("clock_rate", None):
            clock_rate = cfg["clock_rate"]
        else:
            logger.info(
                         "WARNING: clock rate not provided! arbitrarily set to 2.5GHz")
            clock_rate = 2.5e9

    # read the data of the json file..
    with open(filename) as json_file:
        json_data = json.load(json_file)

    # Detect the proper io_keys (lustre or not) as they have a different name in the MAP logs
    _samples = json_data['profile']['samples']

    if _samples.get("lustre_bytes_read") and _samples.get("lustre_bytes_written"):

        io_key_write = "lustre_bytes_written"
        io_key_read = "lustre_bytes_read"

    elif _samples.get("bytes_read") and _samples.get("bytes_written"):

        io_key_write = "bytes_written"
        io_key_read = "bytes_read"

    else:
        print("The allinea map file does not seem to contain IO traces: i.e. [lustre_]bytes_[written|read]")
        sys.exit(1)

    allinea_time_signal_map = {
        'instr_fp':             {'name': 'flops',                          'scale_factor': clock_rate, 'is_time_percent': True},
        io_key_read:            {'name': 'kb_read',       'is_rate': True, 'scale_factor': 1. / 1024.},
        io_key_write:           {'name': 'kb_write',      'is_rate': True, 'scale_factor': 1. / 1024.},
        'mpi_p2p':              {'name': 'n_pairwise',    'is_rate': True},
        'mpi_p2p_bytes':        {'name': 'kb_pairwise',   'is_rate': True, 'scale_factor': 1./1024.},
        'mpi_collect':          {'name': 'n_collective',  'is_rate': True},
        'mpi_collect_bytes':    {'name': 'kb_collective', 'is_rate': True, 'scale_factor': 1./1024.}
    }

    # A quick sanity check
    for value in allinea_time_signal_map.values():
        assert value['name'] in signal_types

    # # fill in the workload structure
    # i_job = IngestedJob()

    # time_start = json_data_stats['profile']['timestamp']
    # runtime = float(json_data_stats['profile']['runtime_ms']) / 1000.
    # time_start_epoch = (datetime.strptime(time_start, "%a %b %d %H:%M:%S %Y") -
    #                     datetime(1970, 1, 1)).total_seconds()

    # fill in the workload structure
    i_job = IngestedJob()

    time_start = json_data['profile']['timestamp']
    runtime = float(json_data['profile']['runtime_ms']) / 1000.

    try_formats = ["%a %b %d %H:%M:%S %Y", "%Y-%m-%dT%H:%M:%S+00", "%Y-%m-%dT%H:%M:%S"]
    time_start_epoch = None
    for fmt in try_formats:
        try:
            time_start_epoch = datetime.strptime(time_start, fmt).timestamp()
            break
        except ValueError:
            continue
    if time_start_epoch is None:
        raise ValueError(f"cannot parse timestamp {time_start_epoch!r}")


    # this job might not necessarily been queued
    i_job.time_created = time_start_epoch - 3
    i_job.time_queued = time_start_epoch - 2
    i_job.time_eligible = time_start_epoch - 1
    i_job.time_start = time_start_epoch
    i_job.runtime = runtime

    i_job.time_end = time_start_epoch + runtime
    i_job.time_in_queue = i_job.time_start - i_job.time_queued

    # Threads are not considered for now..
    i_job.nnodes = int(json_data['profile']["nodes"])
    i_job.ncpus = int(json_data['profile']['targetProcs'])

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
    i_job.queue_type = None

    # # times relative to start of log
    # profiler jobs are considered as if they were started at T0
    # TODO: find more sensible solution to that..
    i_job.time_start_0 = 0.0

    # Obtain the timestamps for the (end of) each sampling window, converted into seconds.
    sample_times = np.array(json_data['profile']['sample_times']) / 1000.
    sample_interval = json_data['profile']['sample_interval'] / 1000.

    for ts_name_allinea, ts_config in allinea_time_signal_map.items():

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

        if ts_config.get('is_time_percent', False):
            y_vals *= sample_interval/100.

        # special case: flops areestimated by (cpu_percent*fp_percent*FREQ*Dt/100)
        if ts_name_allinea == 'instr_fp':
            y_vals *= np.array([v[2]/100. for v in json_data['profile']['samples']['cpu_time_percentage']])

        ts = TimeSignal.from_values(ts_config['name'], sample_times, y_vals,
                                    priority=allinea_signal_priorities [ts_config['name']])
        # if jobs_n_bins is not None:
        #     ts.digitized(nbins=jobs_n_bins)
        i_job.append_time_signal(ts)

    return i_job


def read_allinea_logs(log_dir, cfg=None, jobs_n_bins=None, list_json_files=None):

    """
    Collect info from Allinea logs
    """
    # pick up the list of json files to process
    if list_json_files is None:
        pattern = "**/*.json"
        if cfg is not None and 'pattern' in cfg:
            pattern = cfg.get('pattern')
        json_files = list(pathlib.Path(log_dir).rglob(pattern))
        json_files.sort()
    else:
        json_files = [log_dir+'/'+ff for ff in list_json_files]
        json_files.sort()

    return [ read_allinea_log(filename, jobs_n_bins, cfg=cfg) for filename in json_files ]


class AllineaDataSet(IngestedDataSet):

    def __init__(self, joblist, *args, **kwargs):
        super(AllineaDataSet, self).__init__(joblist, '.', {'cache':False})

        # The created times are all in seconds since an arbitrary reference, so we want to get
        # them relative to a zero-time
        self.global_start_time = min((j.time_created for j in self.joblist))
        self.json_label_map = kwargs.get('json_label_map', None)

    def model_jobs(self):
        for job in self.joblist:
            assert isinstance(job, IngestedJob)

            yield ModelJob(
                job_name=job.jobname,
                user_name=job.user,
                queue_name=job.queue_type,
                cmd_str=job.cmd_str,
                time_queued=job.time_queued,
                time_start=job.time_start,
                duration=job.time_end - job.time_start,
                ncpus=job.ncpus,
                nnodes=job.nnodes,
                stdout=job.stdout,
                label=self.json_label_map[os.path.basename(job.jobname)] if self.json_label_map else None,
                timesignals=job.timesignals,
            )


def ingest_allinea_profiles(path, cfg=None, jobs_n_bins=None, list_json_files=None, json_label_map=None):
    """
    Does what it says on the tin.
    """
    if not os.path.exists(path):
        raise ConfigurationError("Specified path to ingest Allinea profiles does not exist: {}".format(path))

    if not list_json_files:
        if os.path.isdir(path):
            jobs = read_allinea_logs(path,
                                     cfg=cfg,
                                     jobs_n_bins=jobs_n_bins)
        else:
            jobs = [read_allinea_log(path,
                                     cfg=cfg,
                                     jobs_n_bins=jobs_n_bins)]
    else:
        jobs = read_allinea_logs(path,
                                 cfg=cfg,
                                 jobs_n_bins=jobs_n_bins,
                                 list_json_files=list_json_files)

    if not jobs:
        raise RuntimeError("No file found")

    return AllineaDataSet(jobs, json_label_map=json_label_map)


