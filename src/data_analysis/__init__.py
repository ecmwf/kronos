import numpy as np

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


def jobs_to_matrix(input_jobs):
    """
    Create matrix of values from input jobs
    :param input_jobs: list of model jobs
    :return: matrix of job matrices
    """

    # check that is a list of ModelJobs
    assert all(isinstance(job, ModelJob) for job in input_jobs)

    # matrix is built from the timesignals SUMS plus n_procs, n_nodes, runtime
    input_jobs_matrix = np.zeros((len(input_jobs), len(signal_types) + 3))
    for cc, j in enumerate(input_jobs):
        row = [ts.sum if ts is not None else 0 for tsk, ts in j.timesignals.items()]
        row += [j.ncpus, j.nnodes, j.duration]
        input_jobs_matrix[cc, :] = np.asarray(row)

    return input_jobs_matrix


def apply_matrix_to_jobs(input_jobs, metrics_mat):
    """
    Apply suggested metrics to list of model jobs
    :param input_jobs: input jobs that will receive the suggestions
    :param metrics_mat: matrix of metrics
    :return:
    """
    n_ts_values = 5

    # check that is a list of ModelJobs
    assert all(isinstance(job, ModelJob) for job in input_jobs)

    if metrics_mat.shape[0] != len(input_jobs):
        raise ValueError("Metrics size 0 and job list length are not equal!")

    for rr, job in enumerate(input_jobs):

        # row relative to the job
        row = metrics_mat[rr, :]

        # Replace None signals with recommended values
        for tt, ts_sum in enumerate(row[:len(signal_types)]):

            ts_name = signal_types.keys()[tt]
            if job.timesignals[ts_name] is None:
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(n_ts_values),
                                                     np.ones(n_ts_values) * ts_sum/n_ts_values)
                job.timesignals[ts_name] = ts

    return input_jobs

