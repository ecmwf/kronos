# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import math
import datetime

import numpy as np

from kronos.io.definitions import kresults_stats_info


def add_value_to_sublist(_list, idx1, idx2, val):

    assert idx1 >= 0
    assert idx2 <= len(_list)
    assert idx1 <= idx2
    assert isinstance(idx1, int) and isinstance(idx2, int)

    _list[idx1:idx2] = [_list[i]+val for i in range(idx1, idx2)]
    return _list


def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970, 1, 1)).total_seconds()


def cumsum(input_list):
    # return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]
    return np.cumsum(input_list)


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]


def mean_of_list(vals):
    """
    avg value of a list
    :param vals:
    :return:
    """

    return sum(vals)/float(len(vals)) if vals else 0


def std_of_list(vals):
    """
    std of a list
    :param vals:
    :return:
    """
    if not vals:

        return 0

    else:
        mean_val = mean_of_list(vals)
        return (sum([(v-mean_val)**2 for v in vals])/float(len(vals)))**0.5


def sum_of_squared(vals):
    """
    Returns the sum of squared values
    :param vals:
    :return:
    """

    return sum([v**2 for v in vals])


def calc_histogram(values, n_bins):

    assert isinstance(n_bins, int)
    assert n_bins > 0

    max_val = max(values)
    min_val = min(values)

    if max_val == min_val:
        return [min_val, max_val], [len(values)]
    else:
        delta_bin = (max_val - min_val) / float(n_bins)

        bins = [min_val + i * delta_bin for i in range(n_bins + 1)]
        binned_vals = [0] * n_bins

        for val in values:
            idx_min = int(min(max(math.floor( (val-min_val) / float(delta_bin)), 0), len(binned_vals)-1))

            try:
                binned_vals[idx_min] += 1
            except IndexError:
                raise IndexError

        return bins, binned_vals


def print_formatted_class_stats(class_name, per_class_job_stats):

    # Show summary per class
    _fl = 25
    n_fields = 3

    ordered_keys = [
        "read",
        "write",
        "mpi-collective",
        "mpi-pairwise",
        "cpu",
        "file-flush"
    ]

    keys_units = {
        "read": "[GiB]",
        "write": "[GiB]",
        "mpi-collective": "[GiB]",
        "mpi-pairwise": "[GiB]",
        "cpu": "  [G]",
        "file-flush": "  [-]"
    }

    # get the stats for this class (if present..)
    class_stats = per_class_job_stats.get(class_name)

    if class_stats:

        # Header
        print "{}".format("-" * (_fl + 1) * n_fields)
        print "{:<{l}s}|{:^{l}s}|{:^{l}s}|".format("Name", "Total", "Total Time", l=_fl)
        print "{}".format("-" * (_fl + 1) * n_fields)

        # Print the relevant metrics for each stats class
        for k in ordered_keys:

            if k in class_stats.keys():
                v = class_stats[k]
                stats_metric_info = kresults_stats_info[k]

                # retrieve the relevant info from the metric type
                counter_to_print = v[stats_metric_info["label_metric"]]
                conv_fact = stats_metric_info["conv"]
                elaps_time = v["elapsed"]

                print "{:<{l}s}|{:>{lmu}.2f} {}|{:>{lmfour}.2f} [s]|".format(k,
                                                                               counter_to_print * conv_fact,
                                                                               keys_units[k],
                                                                               elaps_time,
                                                                               l=_fl,
                                                                               lmfour=_fl-4,
                                                                               lmu=_fl-len(keys_units[k])-1)

        # h-line
        print "{}".format("-" * (_fl + 1) * n_fields)
    else:
        print "\n\n*** Warning ***: no jobs found in class"


def digitize_xyvalues(xvalues, yvalues, nbins=None, key="sum"):
    """
    On-the-fly return digitized time series (rather than using
    """

    if nbins is None:
        return xvalues, yvalues

    # some checks..
    assert len(xvalues) == len(yvalues)
    nbins = int(nbins)
    xvalues = np.asarray(xvalues)
    yvalues = np.asarray(yvalues)

    # Determine the bin boundaries
    xedge_bins = np.linspace(min(xvalues), max(xvalues) + 1.0e-6, nbins + 1)

    # Return xvalues as the midpoints of the bins
    bins_delta = xedge_bins[1] - xedge_bins[0]
    xvalues_bin = xedge_bins[1:] - (0.5 * bins_delta)

    # Split the data up amongst the bins
    # n.b. Returned indices will be >= 1, as 0 means to the left of the left-most edge.
    bin_indices = np.digitize(xvalues, xedge_bins)

    yvalues_bin = np.zeros(nbins)
    for i in range(nbins):
        if any(yvalues[bin_indices == i+1]):
            if key == 'mean':
                val = yvalues[bin_indices == i+1].mean()
            elif key == 'sum':
                val = yvalues[bin_indices == i+1].sum()
            else:
                raise ValueError("Digitization key value not recognised: {}".format(key))

            yvalues_bin[i] = val

    return xvalues_bin, yvalues_bin

