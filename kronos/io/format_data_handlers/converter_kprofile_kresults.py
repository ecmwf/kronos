# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import strict_rfc3339

from kronos.io.definitions import kresults_ts_names_map, kprofile2kresults_ts_names_map
from kronos.io.results_format import ResultsFormat
from kronos.shared_tools.shared_utils import mean_of_list, std_of_list, sum_of_squared, digitize_xyvalues


class ConverterKprofileKresults(object):

    def __init__(self, kprofiler_data, user_runtime=None):

        self.kprofiler_data = kprofiler_data

        # if set, this tuple overrides the t_start and the t_end of the profile
        # (to be used to "substitute" missing kprofiles with existing ones by adjusting the timestamps only..)
        if user_runtime:
            if user_runtime < 0:
                raise ValueError("user_runtime must be > 0 !")
            else:
                self.user_runtime = user_runtime
        else:
            self.user_runtime = None

    def convert(self, nbins=None):

        kresults_jobs = []

        # profiled jobs
        for prof_job in self.kprofiler_data.profiled_jobs:

            # if user_runtime is set, rescale the job timestamps to be consistent to it..
            if self.user_runtime:
                time_scaling = self.user_runtime / float(prof_job["duration"])
                for tsk, tsv in prof_job["time_series"].iteritems():
                    tsv["times"] = [t * time_scaling for t in tsv["times"]]

            n_procs = prof_job['ncpus']

            # ****** statistics will be distributed per rank *******
            # NOTE: this implies the assumption that all the processes are doing the exact same thing
            # in other words we loose any per-process information (this is acceptable for now but must be clear)
            kprofile_ts_names = prof_job["time_series"].keys()

            # per process time_series
            kres_proc_ts = {}
            kres_proc_times = []
            kres_proc_ts.update({kprofile2kresults_ts_names_map[k]: [] for k in kprofile_ts_names})
            kres_proc_ts["durations"] = []

            # main loop over kprofile metrics
            for ts_name, ts_values in prof_job["time_series"].iteritems():

                # each value has to be appended to every series (as defined in the kprofile format)
                t_vals, v_vals = digitize_xyvalues(ts_values["times"], ts_values["values"], nbins=nbins, key="sum")

                # retain only non zero-values
                t_vals = [t for tt,t in enumerate(t_vals) if v_vals[tt]]
                v_vals = [v for vv, v in enumerate(v_vals) if v_vals[vv]]

                for tt, t in enumerate(t_vals):

                    kres_proc_times.append(t)
                    v = v_vals[tt]

                    _dt = t_vals[tt]-t_vals[tt-1] if tt else t_vals[tt]
                    kres_proc_ts["durations"].append(_dt)

                    for kprof_metric in kprofile_ts_names:

                        # get kresult name and proper scaling factor
                        kres_name = kprofile2kresults_ts_names_map[kprof_metric]
                        kres_val = v if kprof_metric == ts_name else 0
                        if kres_val:
                            kres_val /= kresults_ts_names_map[kres_name][1]
                            kres_val /= n_procs if not kresults_ts_names_map[kres_name][2] else 1.0
                        kres_proc_ts[kres_name].append(kres_val)

            # define required time series that are missing
            kres_proc_ts.update(self.missing_timeseries(kres_proc_ts))

            # Each timeseries needs to be sorted according to timestamps before calculating durations
            for kres_name in kres_proc_ts.keys():
                sorted_ts = [ts_v for _, ts_v in sorted(zip(kres_proc_times, kres_proc_ts[kres_name]))]
                kres_proc_ts[kres_name] = sorted_ts

            # calculate durations from timestamps
            kres_proc_times.sort()
            # kres_proc_ts["durations"] = [v_i - v_ii for v_i, v_ii in zip(kres_proc_times, [0]+kres_proc_times[:-1])]

            # now calculate the "rank" entries of the json file
            ranks_data = []
            for rr, rank in enumerate(range(n_procs)):
                rank_data = {
                    "rank": rr,
                    "pid": rr,
                    "host": "none",
                    "stats": self.calc_stats_from_kresults_timeseries(kres_proc_ts),
                    "time_series": kres_proc_ts
                }

                ranks_data.append(rank_data)

            # then aggregates the kresults into kresults_Data
            kresults_jobs.append(ResultsFormat(ranks_data, created=strict_rfc3339.now_to_rfc3339_utcoffset(), uid=9999))

            return kresults_jobs


    @classmethod
    def missing_timeseries(cls, kresults_perproc_timeseries):
        """
        Define required time series and replace them in case they are missing
        :param kresults_perproc_timeseries:
        :return:
        """

        required_ts = {
            "bytes_write": {"name": "n_write", "default": 1, "type": int},
            "bytes_read": {"name": "n_read", "default": 1, "type": int},
            "bytes_pairwise": {"name": "n_pairwise", "default": 1, "type": int},
            "bytes_collective": {"name": "n_collective", "default": 1, "type": int},
        }

        missing_time_series = {}
        for ts_name, ts_values in kresults_perproc_timeseries.iteritems():
            if ts_name in required_ts and not kresults_perproc_timeseries.get(required_ts[ts_name]["name"]):

                ts_typ = required_ts[ts_name]["type"]
                ts_def = required_ts[ts_name]["default"]
                ts_name = required_ts[ts_name]["name"]
                missing_time_series[ts_name] = [ts_typ(ts_def) if v else 0 for v in ts_values]

        return missing_time_series


    @classmethod
    def calc_stats_from_kresults_timeseries(cls, kresults_perproc_timeseries):

        stats_data = {}

        for kres_ts_name, kres_ts_vals in kresults_perproc_timeseries.iteritems():

            if kres_ts_name == "flops":
                _non_null_vals = [v for v in kres_ts_vals if v]
                _non_null_elapsed = [d for d, v in zip(kresults_perproc_timeseries["durations"], kres_ts_vals) if v]
                stats_data["cpu"] = {
                    "stddevElapsed": std_of_list(_non_null_elapsed),
                    "sumSquaredElapsed": sum([v*v for v in _non_null_elapsed]),
                    "averageElapsed": mean_of_list([el/val for el, val in zip(_non_null_elapsed, _non_null_vals) if val]),
                    "elapsed": sum(_non_null_elapsed),
                    "count": int(sum(_non_null_vals))
                }

            if kres_ts_name == "bytes_read":
                _non_null_vals = [v for v in kres_ts_vals if v]
                _non_null_elapsed = [d for d, v in zip(kresults_perproc_timeseries["durations"], kres_ts_vals) if v]
                _non_null_count = [v for v in kresults_perproc_timeseries["n_read"] if v]
                stats_data["read"] = {
                    "stddevElapsed": std_of_list(_non_null_elapsed),
                    "sumSquaredElapsed": sum_of_squared(_non_null_elapsed),
                    "averageElapsed": sum(_non_null_elapsed) / sum(_non_null_count),
                    "elapsed": sum(_non_null_elapsed),
                    "stddevBytes": std_of_list(_non_null_vals),
                    "sumSquaredBytes": sum_of_squared(_non_null_vals),
                    "averageBytes": sum(_non_null_vals) / sum(_non_null_count),
                    "bytes": sum(_non_null_vals),
                    "count": int(sum(_non_null_count))
                }

            if kres_ts_name == "bytes_write":
                _non_null_vals = [v for v in kres_ts_vals if v]
                _non_null_elapsed = [d for d, v in zip(kresults_perproc_timeseries["durations"], kres_ts_vals) if v]
                _non_null_count = [v for v in kresults_perproc_timeseries["n_write"] if v]
                stats_data["write"] = {
                    "stddevElapsed": std_of_list(_non_null_elapsed),
                    "sumSquaredElapsed": sum_of_squared(_non_null_elapsed),
                    "averageElapsed": sum(_non_null_elapsed) / sum(_non_null_count),
                    "elapsed": sum(_non_null_elapsed),
                    "stddevBytes": std_of_list(_non_null_vals),
                    "sumSquaredBytes": sum_of_squared(_non_null_vals),
                    "averageBytes": sum(_non_null_vals) / sum(_non_null_count),
                    "bytes": sum(_non_null_vals),
                    "count": int(sum(_non_null_count))
                }

            if kres_ts_name == "bytes_pairwise":

                _non_null_vals = [v for v in kres_ts_vals if v]
                _non_null_elapsed = [d for d, v in zip(kresults_perproc_timeseries["durations"], kres_ts_vals) if v]
                _non_null_count = [v for v in kresults_perproc_timeseries["n_pairwise"] if v]

                stats_data["mpi-pairwise"] = {
                    "stddevElapsed": std_of_list(_non_null_elapsed),
                    "sumSquaredElapsed": sum_of_squared(_non_null_elapsed),
                    "averageElapsed": sum(_non_null_elapsed) / sum(_non_null_count),
                    "elapsed": sum(_non_null_elapsed),
                    "stddevBytes": std_of_list(_non_null_vals),
                    "sumSquaredBytes": sum_of_squared(_non_null_vals),
                    "averageBytes": sum(_non_null_vals) / sum(_non_null_count),
                    "bytes": sum(_non_null_vals),
                    "count": int(sum(_non_null_count))
                }

            if kres_ts_name == "bytes_collective":

                _non_null_vals = [v for v in kres_ts_vals if v]
                _non_null_elapsed = [d for d, v in zip(kresults_perproc_timeseries["durations"], kres_ts_vals) if v]
                _non_null_count = [v for v in kresults_perproc_timeseries["n_collective"] if v]

                stats_data["mpi-collective"] = {
                    "stddevElapsed": std_of_list(_non_null_elapsed),
                    "sumSquaredElapsed": sum_of_squared(_non_null_elapsed),
                    "averageElapsed": sum(_non_null_elapsed) / sum(_non_null_count),
                    "elapsed": sum(_non_null_elapsed),
                    "stddevBytes": std_of_list(_non_null_vals),
                    "sumSquaredBytes": sum_of_squared(_non_null_vals),
                    "averageBytes": sum(_non_null_vals) / sum(_non_null_count),
                    "bytes": sum(_non_null_vals),
                    "count": int(sum(_non_null_count))
                }

        return stats_data
