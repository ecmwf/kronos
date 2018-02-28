# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from collections import OrderedDict

# map that translates KResults and KProfile metrics
kresults_ts_names_map = {
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

kresults_stats_info = OrderedDict((
    ("cpu", {"conv": 1.0/1000.0**3,
             "label_sum": "FLOPS",
             "label_rate": "FLOPS [GB/sec]",
             "label_metric": "count",
             "to_sum": ["count", "elapsed"],
             "def_rate": ["count", "elapsed"],
             }),

    ("read", {"conv": 1.0/1024.0**3,
              "label_sum": "I/O read",
              "label_rate": "I/O read [GiB/sec]",
              "label_metric": "bytes",
              "to_sum": ["count", "elapsed", "bytes"],
              "def_rate": ["bytes", "elapsed"],
              }),

    ("write", {"conv": 1.0/1024.0**3,
               "label_sum": "I/O write",
               "label_rate": "I/O write [GiB/sec]",
               "label_metric": "bytes",
               "to_sum": ["count", "elapsed", "bytes"],
               "def_rate": ["bytes", "elapsed"],
               }),

    ("mpi-pairwise", {"conv": 1.0/1024.0**3,
                      "label_sum": "MPI p2p",
                      "label_rate": "MPI p2p [GiB/sec]",
                      "label_metric": "bytes",
                      "to_sum": ["count", "elapsed", "bytes"],
                      "def_rate": ["bytes", "elapsed"],
                      }),

    ("mpi-collective", {"conv": 1.0/1024.0**3,
                        "label_sum": "MPI col",
                        "label_rate": "MPI col [GiB/sec]",
                        "label_metric": "bytes",
                        "to_sum": ["count", "elapsed", "bytes"],
                        "def_rate": ["bytes", "elapsed"],
                        })
))


sorted_kresults_stats_names = kresults_stats_info.keys()
