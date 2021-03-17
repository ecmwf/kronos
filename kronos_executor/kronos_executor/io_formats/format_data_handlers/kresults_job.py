# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import re
from datetime import datetime

from kronos_executor.io_formats.results_format import ResultsFormat
from kronos_executor.io_formats.definitions import kresults_ts_names_map

from kronos_executor.tools import cumsum


class KResultsJob(object):
    """
    This class defines a job that is run and self-profiled by Kronos.
    It mainly defines wrapping methods on top of the KResults data..
    """

    def __init__(self, _json_data, decorator_data=None):

        # JSON data of the KResults file
        self._json_data = _json_data

        # Decorating data (if available)
        self.decorating_data = decorator_data

        # Calculate time series of this job
        self.time_series = self.calc_time_series()

    def calc_time_series(self):

        _series = {}
        if "global" in self._json_data and "time_series" in self._json_data["global"]:
            global_time_series = self._json_data["global"]["time_series"]

            delta_t = global_time_series['durations']
            tends = cumsum(delta_t)

            for ts_name, ts_vals in global_time_series.items():
                if ts_name != "durations":
                    ts_all = [(t, v, v / dt, dt) for t, dt, v in zip(tends, delta_t, ts_vals) if (v != 0 and dt != 0)]
                    _series[ts_name] = ts_all

        _series_tvr = {}
        if "ranks" in self._json_data:
            # group time series from kresults data..
            for rr,rank_data in enumerate(self._json_data["ranks"]):

                delta_t = rank_data["time_series"]['durations']
                tends = cumsum(delta_t)

                for ts_name, ts_vals in rank_data["time_series"].items():

                    if ts_name != "durations":
                        ts_all = [(t, v, v / dt, dt) for t, dt, v in zip(tends, delta_t, ts_vals) if (v != 0 and dt != 0)]
                        _series_tvr.setdefault(ts_name, []).extend(ts_all)

            # sort the time-series with ascending time..
            for ts in _series_tvr.values():
                ts.sort(key=lambda _x: _x[0])

        # Per-rank time series override the global ones
        _series.update(_series_tvr)

        # Append any time series data that is present
        time_series = {}
        for name, values in _series.items():

            if values:
                ts_t, ts_v, ts_r, ts_e = zip(*values)
                time_series[kresults_ts_names_map[name][0]] = {
                    'times': list(ts_t),
                    'values': [v * kresults_ts_names_map[name][1] for v in list(ts_v)],
                    'ratios': [v * kresults_ts_names_map[name][1] for v in list(ts_r)],
                    'elapsed': list(ts_e),
                }

        return time_series

    def calc_metrics_sums(self):
        """
        REturn metrics sums
        :return:
        """
        _series = self.calc_time_series()
        return {k: sum(v["values"]) for k, v in _series.items()}

    @classmethod
    def from_kresults_file(cls, kresults_filename, decorator=None):
        """
        instantiate a KResultsJob object from name of KResults file
        :param kresults_filename:
        :param decorator:
        :return:
        """

        return cls(
            ResultsFormat.from_filename(kresults_filename, validate_json=False).output_dict(),
            decorator_data=decorator
        )

    def get_stats(self):
        """
        Get stats for a specific metric (list of per-process stats)
        :param metric_name:
        :return:
        """

        # List of stats fields for each process
        return [rank["stats"] for rank in self._json_data["ranks"]]

    def profiled_metrics(self):
        """
        Return the statistics as in the KResults data
        :return:
        """
        return [r["stats"] for r in self._json_data["ranks"]]

    def get_metric(self, metric_name):
        pass

    @property
    def n_cpu(self):
        """
        retrieve n cpu from the number of ranks
        :return:
        """
        if self.decorating_data:
            if hasattr(self.decorating_data, "ncpus"):
                assert self.decorating_data.ncpus == len(self._json_data["ranks"]), "N CPU mismatch: check job data!"

        return len(self._json_data["ranks"])

    @property
    def label(self):
        """
        Job label
        :return:
        """
        return self.decorating_data.label if self.decorating_data else None

    @property
    def name(self):
        """
        Job name
        :return:
        """
        return self.decorating_data.name if self.decorating_data else None

    @property
    def t_start(self):

        # T_start is calculated from t_end
        return self.t_end - self.duration

    @property
    def duration(self):
        return max([v for ts in self.time_series.values() for v in ts["times"]])

    @property
    def t_end(self):

        # t_end is defined as the creation timestamp of KResults
        _ts = None
        if self._json_data.get("created"):

            if isinstance(self._json_data["created"], datetime):
                created_datetime = self._json_data["created"]
            else:
                created_datetime = datetime.strptime(self._json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00')

            _ts = created_datetime.timestamp()
        return _ts

    def is_in_class(self, class_regex=None):
        """
        Check if this job belongs to a specified class
        :param class_regex:
        :return:
        """

        # if the class name is not provided, job is set to belong to class by default
        if not class_regex:
            return True

        if not self.name:
            return False

        return True if re.match(class_regex, self.label) else False

    def get_class_name(self, class_list):
        """
        Return a string of the class name
        :return:
        """

        # NOTE: a job can be in multiple classes
        job_classes = [cl_name for cl_name, cl_regex in class_list.items() if self.is_in_class(cl_regex)]

        if job_classes:

            return job_classes

        else:

            # If no specific classes are found, a "generic_class"
            return ["generic_class"]
