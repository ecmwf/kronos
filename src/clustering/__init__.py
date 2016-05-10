from kmeans import ClusteringKmeans
from SOM import ClusteringSOM
from DBSCAN import ClusteringDBSCAN


def factory(key, data):

    workers = {
        "Kmeans": ClusteringKmeans,
        "SOM": ClusteringSOM,
        "DBSCAN": ClusteringDBSCAN
    }

    return workers[key](data)
