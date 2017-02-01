# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np


def find_n_clusters_silhouette(silhouette_scores, n_round_off):

    # round the values to avoid useless repetition of very close values
    # if the values are equal within the selected tolerance, pick up the first occurrrence
    # (which corresponds to the minimum number of clusters)
    round_silhouette_scores = np.round(silhouette_scores, n_round_off)

    # pick up the first occurrence of the max score
    optimal_n_clusters_idx=np.argmax(round_silhouette_scores)

    return optimal_n_clusters_idx
