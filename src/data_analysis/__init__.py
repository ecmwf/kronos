from clust_kmeans import ClusteringKmeans
from clust_DBSCAN import ClusteringDBSCAN


clustering_algorithms = {
    "Kmeans": ClusteringKmeans,
    "DBSCAN": ClusteringDBSCAN
}


def factory(key, config):

    return clustering_algorithms[key](config)
