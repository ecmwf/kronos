import numpy as np


def find_n_clusters_silhouette(silhouette_scores, n_round_off):

    # round the values to avoid useless repetition of very close values
    # if the values are equal within the selected tolerance, pick up the first occurrrence
    # (which corresponds to the minimum number of clusters)
    round_silhouette_scores = np.round(silhouette_scores, n_round_off)

    # pick up the first occurrence of the max score
    optimal_n_clusters_idx=np.argmax(round_silhouette_scores)

    return optimal_n_clusters_idx
