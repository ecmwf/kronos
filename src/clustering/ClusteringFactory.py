from ClusteringKmeans import ClusteringKmeans
from ClusteringSOM import ClusteringSOM
from ClusteringDBSCAN import ClusteringDBSCAN


def ClusteringFactory(key, data, Nclusters):

    if key == "Kmeans":
        
        if Nclusters < 1:
            raise ValueError('N clusters need to be > 1 for method'+key)
        else:                
            return ClusteringKmeans(data)

    elif key == "SOM":
        
        if Nclusters < 1:
            raise ValueError('N clusters need to be > 1 for method'+key)
        else:
            return ClusteringSOM(data)

    elif key == "DBSCAN":

        return ClusteringDBSCAN(data)

    else:

        raise ValueError('option not recognised!')
