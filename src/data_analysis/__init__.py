# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from clust_kmeans import ClusteringKmeans
from clust_DBSCAN import ClusteringDBSCAN


clustering_algorithms = {
    "Kmeans": ClusteringKmeans,
    "DBSCAN": ClusteringDBSCAN
}


def factory(key, config):

    return clustering_algorithms[key](config)
