# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import math
import datetime


def add_value_to_sublist(_list, idx1, idx2, val):
    _list[idx1:idx2] = [_list[i]+val for i in range(idx1, idx2)]
    return _list


def datetime2epochs(t_in):
    return (t_in - datetime.datetime(1970, 1, 1)).total_seconds()


def cumsum(input_list):
    return [sum(input_list[:ii+1]) for ii,i in enumerate(input_list)]


def linspace(x0, x1, count):
    return [x0+(x1-x0)*i/float(count-1) for i in range(count)]


def mean_of_list(vals):
    """
    avg value of a list
    :param vals:
    :return:
    """
    return sum(vals)/float(len(vals))


def std_of_list(vals):
    """
    std of a list
    :param vals:
    :return:
    """
    mean_val = mean_of_list(vals)
    return (sum([(v-mean_val)**2 for v in vals])/float(len(vals)))**0.5


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
            idx_min = int(min(max(math.floor(val / int(delta_bin)), 0), len(binned_vals)-1))

            try:
                binned_vals[idx_min] += 1
            except IndexError:
                raise IndexError

        return bins, binned_vals