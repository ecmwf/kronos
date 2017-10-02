# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import fnmatch
import re
from collections import OrderedDict

from datetime import datetime

from kronos.core.post_process.definitions import cumsum, datetime2epochs, class_names_complete
from kronos.io.results_format import ResultsFormat

ts_names_map = {
    "n_write": ("n_write", 1.0),
    "n_read": ("n_read", 1.0),
    "bytes_write": ("kb_write", 1.0/1024.0),
    "bytes_read": ("kb_read", 1.0/1024.0),
    "n_pairwise": ("n_pairwise", 1.0),
    "bytes_pairwise": ("kb_pairwise", 1.0/1024.0),
    "n_collective": ("n_collective", 1.0),
    "bytes_collective": ("kb_collective", 1.0/1024.0),
    "flops": ("flops", 1.0),
}

krf_stats_info = OrderedDict((
    ("cpu", {"conv": 1.0/1000.0**3,
             "label_sum": "FLOPS",
             "label_rate": "FLOPS [GB/sec]",
             "to_sum": ["count", "elapsed"],
             "def_rate": ["count", "elapsed"],
             }),

    ("read", {"conv": 1.0/1024.0**3,
              "label_sum": "I/O read",
              "label_rate": "I/O read [GiB/sec]",
              "to_sum": ["count", "elapsed", "bytes"],
              "def_rate": ["bytes", "elapsed"],
              }),

    ("write", {"conv": 1.0/1024.0**3,
               "label_sum": "I/O write",
               "label_rate": "I/O write [GiB/sec]",
               "to_sum": ["count", "elapsed", "bytes"],
               "def_rate": ["bytes", "elapsed"],
               }),

    ("mpi-pairwise", {"conv": 1.0/1024.0**3,
                      "label_sum": "MPI p2p",
                      "label_rate": "MPI p2p [GiB/sec]",
                      "to_sum": ["count", "elapsed", "bytes"],
                      "def_rate": ["bytes", "elapsed"],
                      }),

    ("mpi-collective", {"conv": 1.0/1024.0**3,
                        "label_sum": "MPI col",
                        "label_rate": "MPI col [GiB/sec]",
                        "to_sum": ["count", "elapsed", "bytes"],
                        "def_rate": ["bytes", "elapsed"],
                        })
))


sorted_krf_stats_names = krf_stats_info.keys()


class KRFJob(object):
    """
    This class defines a job that is run and self-profiled by Kronos.
    It mainly defines wrapping methods on top of the KRF data..
    """

    def __init__(self, _json_data, decorator_data=None):

        # JSON data of the KRF file
        self._json_data = _json_data

        # Decorating data (if available)
        self.decorating_data = decorator_data

        # Calculate time series of this job
        self.time_series = self.calc_time_series()

    def calc_time_series(self):

        # group time series from krf data..
        _series_tvr = {}
        for rr,rank_data in enumerate(self._json_data["ranks"]):

            delta_t = rank_data["time_series"]['durations']
            tends = cumsum(delta_t)

            for ts_name, ts_vals in rank_data["time_series"].iteritems():

                if ts_name != "durations":
                    ts_all = [(t, v, v / dt, dt) for t, dt, v in zip(tends, delta_t, ts_vals) if (v != 0 and dt != 0)]
                    _series_tvr.setdefault(ts_name, []).extend(ts_all)

        # sort the time-series with ascending time..
        for ts in _series_tvr.values():
            ts.sort(key=lambda _x: _x[0])

        # Append any time series data that is present
        time_series = {}
        for name, values in _series_tvr.iteritems():

            # assert name in time_signal.signal_types
            assert name in ts_names_map.keys()

            if values is not None:
                ts_t, ts_v, ts_r, ts_e = zip(*values)
                time_series[ts_names_map[name][0]] = {
                    'times': list(ts_t),
                    'values': [v * ts_names_map[name][1] for v in list(ts_v)],
                    'ratios': [v * ts_names_map[name][1] for v in list(ts_r)],
                    'elapsed': list(ts_e),
                }

        return time_series

    @classmethod
    def from_krf_file(cls, krf_filename, decorator=None):
        """
        instantiate a KRFJob object from name of KRF file
        :param krf_filename:
        :param decorator:
        :return:
        """

        return cls(
            ResultsFormat.from_filename(krf_filename, validate_json=False).output_dict(),
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
        Return the statistics as in the KRF data
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
        return self.decorating_data.label

    @property
    def name(self):
        """
        Job name
        :return:
        """
        return self.decorating_data.name

    @property
    def t_start(self):

        _ts = None
        if self._json_data.get("created"):

            if isinstance(self._json_data["created"], datetime):
                created_datetime = self._json_data["created"]
            else:
                created_datetime = datetime.strptime(self._json_data["created"], '%Y-%m-%dT%H:%M:%S+00:00')

            _ts = datetime2epochs(created_datetime)
        return _ts

    @property
    def duration(self):
        return max([v for ts in self.time_series.values() for v in ts["times"]])

    @property
    def t_end(self):
        return self.t_start + self.duration

    def is_in_class(self, class_name_idx=None):
        """
        Check if this job belongs to a specified class
        :param class_name_idx:
        :return:
        """

        # if the class name is not provided, job is set to belong to class by default
        if not class_name_idx:
            return True

        # otherwise check if job.label matches the class name
        else:
            class_name = class_name_idx[0]
            class_sp = class_name_idx[1]

            # print "------------------"
            # print "self.label ", self.label
            # print "class_name ", class_name
            # print "class_sp ", class_sp

            name_match = re.match(fnmatch.translate(class_name), self.label)
            sp_match = class_sp in self.label

            # return class_name in self.label and class_sp in self.label
            found_match = (name_match and sp_match) if (name_match and sp_match) else False
            # print found_match
            # print "------------------"
            return found_match

    def get_class_name(self):
        """
        Return a string of the class name
        :return:
        """

        class_name = [class_name for class_name in class_names_complete if self.is_in_class(class_name)]

        if class_name:
            return class_name[0][0] + "/" + class_name[0][1]
        else:
            raise ValueError("class of job {} not found".format(self.label))
