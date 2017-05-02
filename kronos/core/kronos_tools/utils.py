# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import numpy as np
import math

from kronos.core.exceptions_iows import ConfigurationError


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

    # Fix the spurious values for which tend==tstart as they would cause troubles when calculating the running mean
    diff_vec = end_times-start_times
    end_times[diff_vec == 0] += 1

    plus_vec = np.hstack((start_times, vals_vec))
    minus_vec = np.hstack((end_times, -1 * vals_vec))

    # vector of time-stamps (and +-1 for start-end time stamps)
    all_vec = np.vstack((plus_vec, minus_vec))
    all_vec_sort = all_vec[all_vec[:, 0].argsort()]
    time_stamps_calc = all_vec_sort[:, 0]

    running_sum_vec = np.cumsum(all_vec_sort[:, 1])

    return time_stamps_calc, running_sum_vec


def running_series(jobs, times):

    bin_width = times[1] - times[0]

    running = np.zeros(len(times))

    for j in jobs:

        first = max(0, int(math.floor( int(j.time_start - times[0]) / int(bin_width))) )
        last = max(0, int(math.floor( int(j.time_start+j.duration-times[0]) / int(bin_width))) )

        running[first:last] += 1

    return running


def bin_array(t, data, bins_in, mode="sum"):
    """
    Function that returns a binned array (elements that fall within a bin can be either discretized or averaged)
    :param t:
    :param data:
    :param bins: [integer]: bins span the t vector
                 [numpy.ndarray]: bins fully specified
    :param mode: "sum" or "mean"
    :return:
    """

    eps = 1e-8

    t = np.asarray(t)
    data = np.asarray(data)

    if isinstance(bins_in, int):
        bins = np.linspace(min(t)-eps, max(t)+eps, bins_in)
        t_bins = (bins[1:]+bins[:-1])/2.0
    elif isinstance(bins_in, np.ndarray):
        bins = bins_in
        bins[0] -= eps
        bins[-1] += eps
        t_bins = (bins[1:]+bins[:-1])/2.0
    else:
        raise ConfigurationError("bins must be either interger or numpy array!")

    digitized = np.digitize(t, bins)

    # method "sum"
    if mode=="sum":
        bin_values = np.asarray([data[digitized == i].sum() if data[digitized == i].size else 0 for i in range(1, len(bins))])
        bin_values = np.asarray(bin_values)

        # just a check..
        if sum(data)-sum(bin_values) > 1e-10:
            print "different sum! orig: {}, binned: {}".format(sum(data), sum(bin_values))

    # method "mean"
    elif mode == "mean":
        bin_values = np.asarray([data[digitized == i].mean() if data[digitized == i].size else 0 for i in range(1, len(bins))])
        bin_values = np.asarray(bin_values)

    else:
        raise ConfigurationError("mode must be either sum or mean!")

    return t_bins, bin_values


def calculate_signals_similarity(t1, v1, t2, v2):
    """
    Calculate offset of 2 signals that results in best matching of two signals
    :param t1: time signal 1
    :param v1: signal 1
    :param t2: time signal 2
    :param v2: signal 2
    :return: offset relative to len of t1 (==len(t2)) and relative cross_correlation value
    """

    assert len(t1) == len(t2)
    assert len(v1) == len(v2)

    v1_norm = v1/np.linalg.norm(v1)
    v2_norm = v2/np.linalg.norm(v2)

    cross_corr = np.correlate(v1_norm, v2_norm, "full")

    assert len(cross_corr) == 2*len(v1_norm)-1

    if np.argmax(cross_corr) < len(t1):
        max_corr_idx = len(t1) - (np.argmax(cross_corr) + 1)
    else:
        max_corr_idx = (np.argmax(cross_corr) + 1) - len(t1)

    # relative offset is a measure of how much a signal v1 is "shifted" in time WRT v2 (relative to total time length)
    if t1[-1]-t1[0] == 0:
        print "time-signal has zero duration!!"

    relative_time_err_pc = (t1[max_corr_idx] - t1[0])/(t1[-1]-t1[0]) * 100.0

    relative_corr_err_pc = (1.0-np.max(cross_corr)) * 100.0
    if np.isnan(np.max(cross_corr)):
        print "NAN cross-corr"

    return relative_time_err_pc, relative_corr_err_pc


def lin_reg(x_in, y_in, alpha=1e-1, niter=10000):

    """ simple linear regression """
    theta = np.zeros((2, 1))
    cost = 0

    for ii in range(niter):
        cost, grad = calc_grad(x_in, y_in, theta)
        theta = theta - alpha * grad
        print "theta: {} ".format(theta)

    return cost, theta


def calc_grad(x_in, y_in, theta):

    """ grad for linear regression """
    x_in = x_in.reshape([np.asarray(x_in).size, -1])
    y_in = y_in.reshape([np.asarray(y_in).size, -1])

    mm = len(x_in)

    xx = np.hstack((np.ones(x_in.shape), x_in))

    diff_vec = np.dot(xx, theta)-y_in
    cost = 1./(2.*mm) * np.dot(np.transpose(diff_vec), diff_vec)
    grad = 1./mm * np.dot(np.transpose(xx), np.dot(xx, theta)-y_in )

    return cost, grad