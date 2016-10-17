
import os
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from sklearn import cross_validation as cv

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from time_signal import signal_types


def apply_recomm_sys(matching_jobs, acc_model_jobs):

    # add the darshan job records
    all_jobs_data_dsh = np.zeros((len(matching_jobs), len(signal_types) + 3))
    for cc, j in enumerate(matching_jobs):
        # take only non None metrics in the jobs..
        row = [ts.sum if ts is not None else 0 for tsk, ts in j.timesignals.items()]
        row += [j.ncpus, j.nnodes, j.duration]
        all_jobs_data_dsh[cc, :] = np.asarray(row)

    # Normalize all the data
    item_means = np.mean(all_jobs_data_dsh, axis=0)
    all_jobs_data_dsh_norm = all_jobs_data_dsh / item_means[None, :]
    print "created job metrics structure!"

    print "creating user-item and item-item similarities.."
    # use scikit to split the data into training and testing set..
    train_data_matrix, test_data_matrix = cv.train_test_split(all_jobs_data_dsh_norm, test_size=0.25)

    # calculate cosine similarities (user-user matrix and item-item matrix)..
    # these similarity matrices will then be used to predict the missing data for "new users" (jobs..)
    user_similarity_matrix = pairwise_distances(train_data_matrix, metric='cosine')
    item_similarity_matrix = pairwise_distances(train_data_matrix.T, metric='cosine')

    print "user-item matrix size = {}".format(user_similarity_matrix.shape)
    print "item-item matrix size = {}".format(item_similarity_matrix.shape)
    # /////////////////////////////////////////////////////////////////////////

    # //////////// Apply recommender system to accounting jobs.. //////////////
    print "Applying recommender system to accounting jobs.."

    # create database with all accounting job records..

    # add the accounting job records
    acc_jobs_data = np.zeros((len(acc_model_jobs), len(signal_types) + 3))
    for cc, j in enumerate(acc_model_jobs):
        # take only non None metrics in the jobs..
        row = [ts.sum if ts is not None else 0 for tsk, ts in j.timesignals.items()]
        row += [j.ncpus, j.nnodes, j.duration]
        acc_jobs_data[cc, :] = np.asarray(row)

    # use recommender system in accounting logs:
    item_prediction_acc_norm = acc_jobs_data.dot(item_similarity_matrix) / \
                                np.array([np.abs(item_similarity_matrix).sum(axis=1)])

    item_prediction_acc = item_prediction_acc_norm[:, :8] * item_means
    item_prediction_acc = np.vstack((item_prediction_acc, acc_jobs_data[:, 9:11]))

    print "created prediction values for background jobs.."
    # /////////////////////////////////////////////////////////////////////////

    return item_prediction_acc
