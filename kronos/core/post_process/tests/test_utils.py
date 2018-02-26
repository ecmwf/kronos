# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np


def create_kresults_ranks(metrics_calc_all_ranks):
    """
    Function to generate dummy kresults json data from time series..
    """

    ranks_data = []
    rank_id = 0
    pid = 1000
    for metrics_calc in metrics_calc_all_ranks:

        # CPU
        idx_cpu = np.where(metrics_calc["flops"] > 0)
        val_cpu = metrics_calc["flops"][idx_cpu]
        elapsed_cpu = metrics_calc["durations"][idx_cpu]

        # read
        idx_read = np.where(metrics_calc["n_read"] > 0)
        val_bytes_read = metrics_calc["bytes_read"][idx_read]
        val_n_read = metrics_calc["n_read"][idx_read]
        elapsed_read = metrics_calc["durations"][idx_read]

        # write
        idx_write = np.where(metrics_calc["n_write"] > 0)
        val_bytes_write = metrics_calc["bytes_write"][idx_write]
        val_n_write = metrics_calc["n_write"][idx_write]
        elapsed_write = metrics_calc["durations"][idx_write]

        stats_calc = {
            "cpu": {
                "stddevElapsed": -1,  # "-1": used as "not checked/not used for postprocessing.."
                "sumSquaredElapsed": np.sum(elapsed_cpu * elapsed_cpu),
                "averageElapsed": np.mean(elapsed_cpu / val_cpu),
                "elapsed": np.sum(elapsed_cpu),
                "count": sum(metrics_calc["flops"])
            },
            "read": {
                "stddevElapsed": -1,
                "sumSquaredElapsed": -1,
                "averageElapsed": sum(elapsed_read) / sum(val_n_read),
                "elapsed": np.sum(elapsed_read),
                "stddevBytes": -1,
                "sumSquaredBytes": -1,
                "averageBytes": sum(val_bytes_read) / sum(val_n_read),
                "bytes": np.sum(val_bytes_read),
                "count": np.sum(val_n_read)
            },
            "write": {
                "stddevElapsed": -1,
                "sumSquaredElapsed": -1,
                "averageElapsed": sum(elapsed_write) / sum(val_n_write),
                "elapsed": np.sum(elapsed_write),
                "stddevBytes": -1,
                "sumSquaredBytes": -1,
                "averageBytes": sum(val_bytes_write) / sum(val_n_write),
                "bytes": np.sum(val_bytes_write),
                "count": np.sum(val_n_write)
            },
        }

        #         print "====== FLOPS ========"
        #         print "{:25s}{:25s}".format("orig", "calc")
        #         for k in stats_orig["cpu"].keys():
        #             print "{:25s}{:25f}{:25f}".format(k,stats_orig["cpu"][k], stats_calc["cpu"][k])

        #         print "====== READ ========"
        #         print "{:25s}{:25s}".format("orig", "calc")
        #         for k in stats_orig["read"].keys():
        #             print "{:25s}{:25f}{:25f}".format(k,stats_orig["read"][k], stats_calc["read"][k])

        #         print "====== WRITE ========"
        #         print "{:25s}{:25s}{:25s}".format("key", "orig", "calc")
        #         for k in stats_orig["write"].keys():
        #             print "{:25s}{:25f}{:25f}".format(k, stats_orig["write"][k], stats_calc["write"][k])

        # dictionary with all the rank data
        proc_data = {
            "rank": rank_id,
            "pid": pid,
            "host": "dummy_host",
            "statistics": stats_calc,
            "time_series": metrics_calc
        }

        ranks_data.append(proc_data)

        # Update the indices..
        rank_id += 1
        pid += 1

    # return the list of rank data
    return ranks_data


def create_kresults(list_metrics, creation_date=None):
    """
    Creates a dummy KResults from a list of dictionaries, format example below..:
    [
      {
        "flops":       np.asarray([0,0,0,1]),
        "bytes_read":  np.asarray([0,0,0,1]),
        "n_read":      np.asarray([0,0,0,1]),
        "bytes_write": np.asarray([0,0,0,1]),
        "n_write":     np.asarray([0,0,0,1]),
        "durations":   np.asarray([0.1,0.22,0.33,0.22])
      },
      {
        "flops":       np.asarray([0,0,0,1]),
        "bytes_read":  np.asarray([0,0,0,1]),
        "n_read":      np.asarray([0,0,0,1]),
        "bytes_write": np.asarray([0,0,0,1]),
        "n_write":     np.asarray([0,0,0,1]),
        "durations":   np.asarray([0.1,0.22,0.33,0.22])
      },
      {
        ...
      }
    ]
    :param list_metrics:
    :param creation_date
    :return:
    """

    ranks_data = create_kresults_ranks(list_metrics)

    # template for a dummy json KResults file
    kresults_data = {
        "created": creation_date if creation_date else "2017-07-31T01:28:42+00:00",  # just an example creation date
        "ranks": ranks_data,
        "kronosSHA1": "",
        "kronosVersion": "0.1.2",
        "version": 1,
        "tag": "KRONOS-KRESULTS-MAGIC",
        "uid": 10360
    }

    return kresults_data
