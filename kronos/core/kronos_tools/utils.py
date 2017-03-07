# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import numpy as np


def mb(n_bytes):
    """ Mb from bytes """
    return n_bytes / 1024 / 1024


def freq_to_time(time, freqs, ampls, phases):
    """ from frequency to time domain """
    time_signal = np.zeros(len(time))
    for iF in np.arange(0, len(freqs)):
        time_signal = time_signal + \
                      ampls[iF] * np.sin(2 * np.pi * freqs[iF] * time + phases[iF])
    return time_signal


def sort_dict_list(d, sorted_keys_list):
    return [d[i] for i in sorted_keys_list]


def running_sum(t0_vec, t1_vec, vals):
    """
    Calculate running cumulative sums of vectors (e.g. type y(t))
    :param t0_vec:
    :param t1_vec:
    :param vals:
    :return:
    """

    # calculate running jobs in time..
    start_times = t0_vec.reshape(t0_vec.shape[0], 1)
    end_times = t1_vec.reshape(t1_vec.shape[0], 1)
    vals_vec = vals.reshape(vals.shape[0], 1)

    plus_vec = np.hstack((start_times, vals_vec))
    minus_vec = np.hstack((end_times, -1 * vals_vec))

    # vector of time-stamps (and +-1 for start-end time stamps)
    all_vec = np.vstack((plus_vec, minus_vec))
    all_vec_sort = all_vec[all_vec[:, 0].argsort()]
    time_stamps_calc = all_vec_sort[:, 0]

    running_sum_vec = np.cumsum(all_vec_sort[:, 1])

    return time_stamps_calc, running_sum_vec

