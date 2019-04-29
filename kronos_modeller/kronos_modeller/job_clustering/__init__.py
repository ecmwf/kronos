import logging
from kronos_modeller.job_clustering.kmeans_cluster import KmeansClusters

logger = logging.getLogger(__name__)

clustering_types = {
    "Kmeans": KmeansClusters,
}

