from datetime import datetime
import os, json

from real_job import RealJob
from time_signal import TimeSignal


# collect info from Allinea logs
def read_allinea_logs(log_dir, Jobs_Nbins):

    allinea_jobs = []

    json_files = [pos_json for pos_json in os.listdir(log_dir) if pos_json.endswith('.json')]

    print "reading json files..."

    for js in json_files:

        with open(os.path.join(log_dir, js)) as json_file:
            json_data = json.load(json_file)

        # fill in the workload structure
        i_job = RealJob()

        time_start = json_data['profile']['timestamp']
        runtime = float(json_data['profile']['runtime_ms']) / 1000.
        time_start_epoch = (datetime.strptime(time_start, "%a %b %d %H:%M:%S %Y") -
                            datetime(1970, 1, 1)).total_seconds()

        # this job might not necessarily been queued
        i_job.time_created = time_start_epoch - 3
        i_job.time_queued = time_start_epoch - 2
        i_job.time_eligible = time_start_epoch - 1
        i_job.time_start = time_start_epoch

        i_job.time_end = time_start_epoch + runtime

        i_job.ncpus = 1
        i_job.memory_kb = 0
        i_job.cpu_percent = 0
        i_job.group = ""
        i_job.jobname = ""
        i_job.user = ""

        sample_times = json_data['profile']['sample_times']

        for ts_name in json_data['profile']['samples'].keys():
            name_ts = ts_name
            y_val_bk = json_data['profile']['samples'][ts_name]
            y_val = [v[2] for v in y_val_bk]  # values inside the blocks are: min, max, mean, var
            ts = TimeSignal()
            ts.create_ts_from_values(name_ts, "float", "none", sample_times, y_val)
            ts.digitize(Jobs_Nbins, 'mean')

            # append ts to job
            i_job.append_time_signal(ts)

        # append job to the WL list..
        allinea_jobs.append(i_job)

    return allinea_jobs
