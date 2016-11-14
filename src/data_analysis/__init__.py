import numpy as np
import copy
from clust_kmeans import ClusteringKmeans
from clust_SOM import ClusteringSOM
from clust_DBSCAN import ClusteringDBSCAN

from jobs import ModelJob
from time_signal import TimeSignal
from time_signal import signal_types


clustering_algorithms = {
    "Kmeans": ClusteringKmeans,
    "SOM": ClusteringSOM,
    "DBSCAN": ClusteringDBSCAN
}


def factory(key, config):

    return clustering_algorithms[key](config)


def jobs_to_matrix(input_jobs, n_ts_bins=1):
    """
    Create matrix of values from input jobs
    :param input_jobs: list of model jobs
    :return: matrix of job matrices
    """

    # check that is a list of ModelJobs
    assert all(isinstance(job, ModelJob) for job in input_jobs)

    # append ncpu, nnodes, duration to each row if required
    input_jobs_matrix = np.zeros((len(input_jobs), len(signal_types)*n_ts_bins))

    # loop over jobs and fill the matrix as appropriate
    for cc, job in enumerate(input_jobs):

        row = []
        for tsk in signal_types.keys():
            ts = job.timesignals[tsk]
            if ts is not None:
                xvals, yvals = ts.digitized(n_ts_bins)
                row.extend(yvals)
            else:
                row.extend([0 for vv in range(0,n_ts_bins)])

        input_jobs_matrix[cc, :] = np.asarray(row)

    return input_jobs_matrix


def apply_matrix_to_jobs(input_jobs, metrics_mat):
    """
    Apply suggested metrics to list of model jobs
    :param input_jobs: input jobs that will receive the suggestions
    :param metrics_mat: matrix of metrics
    :return:
    """
    n_ts_values = int(metrics_mat.shape[1]/len(signal_types))
    filled_jobs = copy.deepcopy(input_jobs)

    # check that is a list of ModelJobs
    assert all(isinstance(job, ModelJob) for job in filled_jobs)

    if metrics_mat.shape[0] != len(filled_jobs):
        raise ValueError("Metrics size 0 and job list length are not equal!")

    for rr, job in enumerate(filled_jobs):

        # row relative to the job
        row = metrics_mat[rr, :]

        # Replace None signals with recommended values
        ts_yvalues_all = np.split(row, len(signal_types))
        for tt, ts_vals in enumerate(ts_yvalues_all):

            ts_name = signal_types.keys()[tt]
            if job.timesignals[ts_name] is None:
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(n_ts_values), ts_vals, priority=2)
                job.timesignals[ts_name] = ts

    return filled_jobs

