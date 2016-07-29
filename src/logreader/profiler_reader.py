from datetime import datetime
import os
import json
import getopt

from pybrain.tools.shortcuts import buildNetwork
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer

from jobs import IngestedJob
from time_signal import TimeSignal


def read_allinea_logs(log_dir, jobs_n_bins, list_json_files=None):
    """ Collect info from Allinea logs """

    profiled_jobs = []
    desired2allinea = [('cpu_time',  'instr_fp',             'cpu',        'sum'),
                       ('kb_read',   'lustre_bytes_read',    'file-read',  'sum'),
                       ('kb_write',  'lustre_bytes_written', 'file-write', 'sum'),
                       # ('n_pairwise', 'mpi_p2p', 'mpi', 'sum'),
                       # ('kb_pairwise', 'mpi_p2p_bytes', 'mpi', 'sum'),
                       # ('n_collective', 'mpi_collect', 'mpi', 'sum'),
                       # ('kb_collective', 'mpi_collect_bytes', 'mpi', 'sum')
                       ]

    # pick up the list of json files to process
    if list_json_files is None:
        json_files = [pos_json for pos_json in os.listdir(log_dir) if pos_json.endswith('.json')]
        json_files.sort()
    else:
        json_files = list_json_files
        json_files.sort()

    print "reading json files..."

    for js in json_files:

        with open(os.path.join(log_dir, js)) as json_file:
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
        command_line = json_data['profile']["commandLine"]
        n_nodes = int( command_line.split()[command_line.split().index('-N') + 1])
        n_cpus = int( command_line.split()[command_line.split().index('-n') + 1])
        # n_threads = int( command_line.split()[command_line.split().index('-j') + 1])
        # print "n_cpus: ", n_cpus

        i_job.ncpus = n_cpus
        i_job.nnodes = json_data['profile']["nodes"]

        # average memory used is taken from sample average of "node_mem_percent"
        mem_val_bk = json_data['profile']['samples']['node_mem_percent']
        mem_val = [v[2] for v in mem_val_bk]  # values inside the blocks are: min, max, mean, var
        mem_val_mean = sum(mem_val) / float(len(mem_val))/100.
        mem_node_kb = json_data['profile']["memory_per_node"][2] / 1024.
        i_job.memory_kb = mem_node_kb * mem_val_mean

        i_job.cpu_percent = 0

        i_job.jobname = js
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

            ts_name_std = ts_name_row[0]
            ts_name_allinea = ts_name_row[1]
            ker_type = ts_name_row[2]
            digit_type = ts_name_row[3]

            # if not (name_ker_ts_std == []):
            y_val_bk = json_data['profile']['samples'][ts_name_allinea]
            y_val = [v[2] for v in y_val_bk]  # values inside the blocks are: min, max, mean, var

            # Correct the values of cpu_time by multiplying to total runtime
            if ts_name_std == "cpu_time":
                t_ext = [0] + sample_times
                y_val_ext = [0] + y_val
                y_val = [y_val_ext[tt]*(t_ext[tt]-t_ext[tt-1])/100. for tt in range(1, len(sample_times)+1)]

            # Correct the values for "kb_pairwise" and "kb_collective" (as they are recorded as PER SECOND)
            if ts_name_std in ["kb_pairwise", "kb_collective"]:
                t_ext = [0] + sample_times
                y_val_ext = [0] + y_val
                y_val = [y_val_ext[tt]*(t_ext[tt]-t_ext[tt-1]) for tt in range(1, len(sample_times)+1)]

            if ts_name_std in ["kb_read", "kb_write"]:
                t_ext = [0] + sample_times
                y_val_ext = [0] + y_val
                y_val = [y_val_ext[tt]*(t_ext[tt]-t_ext[tt-1])/1024. for tt in range(1, len(sample_times)+1)]

            # Correct the values for "kb_pairwise" and "kb_collective" (as they are recorded as PER SECOND)
            if ts_name_std in ["n_pairwise", "n_collective"]:
                t_ext = [0] + sample_times
                y_val_ext = [0] + y_val
                y_val = [y_val_ext[tt]*(t_ext[tt]-t_ext[tt-1]) for tt in range(1, len(sample_times)+1)]

            ts = TimeSignal()
            ts.create_ts_from_values(ts_name_std, "float", ker_type, sample_times, y_val)
            ts.digitize(jobs_n_bins, digit_type)
            i_job.append_time_signal(ts)

        # append job to the WL list..
        print "file: ", js, ", read: ", i_job.timesignals[1].sum, " [Kb], write: ", i_job.timesignals[2].sum, " [Kb]"
        profiled_jobs.append(i_job)

    return profiled_jobs
