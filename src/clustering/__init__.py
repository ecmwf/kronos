from kmeans import ClusteringKmeans
from SOM import ClusteringSOM
from DBSCAN import ClusteringDBSCAN

clustering_algorithms = {
    "Kmeans": ClusteringKmeans,
    "SOM": ClusteringSOM,
    "DBSCAN": ClusteringDBSCAN
}


def factory(key, data):

    return clustering_algorithms[key](data)
