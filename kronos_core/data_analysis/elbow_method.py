# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
from kronos_core.plot_handler import PlotHandler


def find_n_clusters(avg_d_in_clust, user_does_not_check):
    """
    Find best number of clusters by elbow method
    :param avg_d_in_clust: average in-cluster distance vector
    :return:
    """

    x = np.arange(avg_d_in_clust.shape[0]) + 1
    popt, pcov = curve_fit(exp_func, x, avg_d_in_clust, p0=(1e8, 0.1, 0))
    yy = exp_func(x, *popt)

    n1 = np.diff(yy)[0]
    p1 = np.asarray([x[0], avg_d_in_clust[0]])
    v1 = np.asarray([1, n1])
    p2 = p1 + v1

    n2 = np.diff(yy)[-1]
    p3 = np.asarray([x[-1], avg_d_in_clust[-1]])
    v2 = np.asarray([1, n2])
    p4 = p3 + v2

    L1 = line(p1, p2)
    L2 = line(p3, p4)
    R = intersection(L1, L2)
    # ======================================

    # ----------- re-project (approx) point on to curve.. ----------------
    dist_vec = [(xp - R[0]) ** 2 + (yy[xp - 1] - R[1]) ** 2 for xp in x]
    nc_x = np.argmin(dist_vec) + 1
    nc_y = yy[np.argmin(dist_vec)]
    # -----------------------------------------------------------------

    # plot elbow method
    plt_hdl = PlotHandler()
    plt.figure(plt_hdl.fig_handle_ID)
    plt.plot(x, avg_d_in_clust, 'k+')
    plt.plot(p1[0], p1[1], 'mo')
    plt.plot(p2[0], p2[1], 'mo')
    plt.plot(p3[0], p3[1], 'bo')
    plt.plot(p4[0], p4[1], 'bo')
    plt.plot(R[0], R[1], 'c', marker='o', markersize=16)
    plt.plot(nc_x, nc_y, 'm', marker='*', markersize=16)
    plt.plot(x, yy, 'r')
    plt.xlabel('N clusters')
    plt.ylabel('mean in-cluster distance')

    # --------- plot intersection to closest point on curve.. ----------
    p0 = R
    p1 = np.asarray([nc_x, nc_y])
    pp = np.asarray([p0 + (p1 - p0) * p for p in np.linspace(0, 1, 10)])
    plt.plot(pp[:, 0], pp[:, 1], 'm--')
    # ------------------------------------------------------------------

    if not user_does_not_check:
        plt.show()

    return int(np.ceil(nc_x))


def line(p1, p2):
    """
    Find line coefficients from point tuples
    :param p1: point 1
    :param p2: point 2
    :return:
    """
    A = (p1[1] - p2[1])
    B = (p2[0] - p1[0])
    C = (p1[0] * p2[1] - p2[0] * p1[1])
    return A, B, -C


def intersection(l1, l2):
    """
    Find line-line intersection
    :param l1: line 1
    :param l2: line 2
    :return:
    """
    D = l1[0] * l2[1] - l1[1] * l2[0]
    Dx = l1[2] * l2[1] - l1[1] * l2[2]
    Dy = l1[0] * l2[2] - l1[2] * l2[0]
    if D != 0:
        x = Dx / D
        y = Dy / D
        return x, y
    else:
        return False


def exp_func(x, a, c, d):
    """
    Exponential curve from parameters
    :param x:
    :param a:
    :param c:
    :param d:
    :return:
    """
    return a * np.exp(-c * x) + d