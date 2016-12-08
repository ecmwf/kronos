import numpy as np
import copy
from clust_kmeans import ClusteringKmeans
from clust_SOM import ClusteringSOM
from clust_DBSCAN import ClusteringDBSCAN

from jobs import ModelJob
from kronos_tools.print_colour import print_colour
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
    non_value_flag = -999

    # loop over jobs and fill the matrix as appropriate
    for cc, job in enumerate(input_jobs):

        row = []
        for tsk in signal_types.keys():
            ts = job.timesignals[tsk]
            if ts is not None:
                xvals, yvals = ts.digitized(n_ts_bins)
                row.extend(yvals)
            else:
                row.extend([non_value_flag for vv in range(0,n_ts_bins)])

        input_jobs_matrix[cc, :] = np.asarray(row)

    # once the matrix is filled, look for missing columns:
    col_size = input_jobs_matrix.shape[1]
    missing_columns = np.zeros(col_size)

    for col_idx in range(col_size):
        if (input_jobs_matrix[:, col_idx] == non_value_flag).all():
            missing_columns[col_idx] = 1

    for tt, ts in enumerate(signal_types.keys()):
        if missing_columns[tt*n_ts_bins]:
            print "Column [{}] has all none values!".format(ts)

    if missing_columns.any():
        # raise ValueError("Some metrics are missing from all the jobs => Recommender system cannot be applied!")
         print_colour("orange", "Some metrics are missing from all the jobs => Recommender system cannot be applied!")

    # # mapping between reduced columns and original columns
    # columns_idxs = {}
    # missing_col_split = np.split(missing_columns, n_ts_bins)
    # for tt, ts_name in enumerate(signal_types.keys()):
    #     if missing_col_split[tt].any() is False:
    #         columns_idxs[ts_name] =
    #
    # # this should be a dictionary {ts_name: [col_idxs]}
    # columns_idxs

    # return input_jobs_matrix[:, missing_columns == 0], columns_idxs
    return input_jobs_matrix


# def apply_matrix_to_jobs(input_jobs, metrics_mat, col_idxs, n_bins, priority):
def apply_matrix_to_jobs(input_jobs, metrics_mat, priority):
    """
    Apply suggested metrics to list of model jobs
    :param input_jobs: input jobs that will receive the suggestions
    :param metrics_mat: matrix of metrics
    :return:
    """
    n_ts_values = int(metrics_mat.shape[1]/len(signal_types))
    # n_ts_values = n_bins
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
            if not job.timesignals[ts_name]:
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(n_ts_values), ts_vals, priority=priority)
                job.timesignals[ts_name] = ts
            elif job.timesignals[ts_name].priority <= priority:
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(n_ts_values), ts_vals, priority=priority)
                job.timesignals[ts_name] = ts
            else:
                pass

    return filled_jobs
