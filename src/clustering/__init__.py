from ClusteringKmeans import ClusteringKmeans
from ClusteringSOM import ClusteringSOM
from ClusteringDBSCAN import ClusteringDBSCAN


def factory(key, data):

    workers = {
        "Kmeans": ClusteringKmeans,
        "SOM": ClusteringSOM,
        "DBSCAN": ClusteringDBSCAN
    }

    return workers[key](data)
