import logging

from kronos_modeller.workload_modelling.clustering.kmeans_cluster import KmeansClusters

logger = logging.getLogger(__name__)

clustering_types = {
    "Kmeans": KmeansClusters,
}

