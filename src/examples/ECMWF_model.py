#!/usr/bin/env python

import os
import cPickle as pickle
import datetime

import math
import numpy as np
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from sklearn import cross_validation as cv
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import pairwise_distances
from scipy.spatial.distance import cdist, pdist

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from time_signal import TimeSignal
from jobs import ModelJob, concatenate_modeljobs
from synthetic_app import SyntheticApp, SyntheticWorkload
from config.config import Config

# Load config
config = Config()

# path_ingested_jobs = "/perm/ma/maab/iows_operational"
path_ingested_jobs = '/perm/ma/maab/ngio_ingested_data/my_ingested'


# ////////////////////////////////////////////////////////////////////////////
def rmse(prediction, ground_truth):
    """ Root mean square error for model evaluation """
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    return math.sqrt(mean_squared_error(prediction, ground_truth))
# ////////////////////////////////////////////////////////////////////////////


# ////////////////////////////////////////////////////////////////////////////
def ecmwf_model():
    # //////////////////////////// ingest logs ///////////////////////////////
    # Darshan
    with open(os.path.join(path_ingested_jobs, "ingested_darshan"), "r") as f:
        darshan_dataset = pickle.load(f)
    print "darshan log data ingested!"

    # IPM
    with open(os.path.join(path_ingested_jobs, "ingested_ipm"), "r") as f:
        ipm_dataset = pickle.load(f)
    print "ipm log data ingested!"

    # stdout
    with open(os.path.join(path_ingested_jobs, "ingested_stdout"), "r") as f:
        stdout_dataset = pickle.load(f)
    print "stdout log data ingested!"
    # /////////////////////////////////////////////////////////////////////////

    # /////////////// matching between darshan, stdout and ipm ////////////////
    print "matching corresponding jobs.."

    # the matching will be focused on darshan records..
    # only 6 hours from the start are retained..
    dsh_times = [job.time_start for job in darshan_dataset.joblist]
    dsh_t0 = min(dsh_times)
    darshan_dataset.joblist = [job for job in darshan_dataset.joblist if job.time_start < (dsh_t0 + 6.0 * 3600.0)]

    # model all jobs
    dsh_model_jobs = [j for j in darshan_dataset.model_jobs()]
    ipm_model_jobs = [j for j in ipm_dataset.model_jobs()]
    std_model_jobs = [j for j in stdout_dataset.model_jobs()]

    matching_jobs = []
    n_match_ipm = 0
    # n_match_std = 0

    for cc, dj in enumerate(dsh_model_jobs):

        if cc % 100 == 0:
            print "job matching iteration: {}".format(cc)

        ipm_job = [ipj for ipj in ipm_model_jobs if
                   (ipj.label + '/serial' == dj.label) or (ipj.label + '/parallel' == dj.label)]

        # merge IPM jobs
        if ipm_job:
            n_match_ipm += 1
            if len(ipm_job) > 1:
                print "error, more than 1 matching job found!!"
            else:
                dj.merge(ipm_job[0])
                matching_jobs.append(dj)

    print "matching={} (out of {})".format(n_match_ipm, float(len(dsh_model_jobs)))
    # /////////////////////////////////////////////////////////////////////////

    # ///////////////// Group operational jobs in the tree ////////////////////
    print "grouping low-level jobs in the tree.."

    n_levels = 3
    # labels_tree = [j.label.split('/') for j in matching_jobs]

    job_groups = {}
    for j in matching_jobs:

        # if it is a deep node, group it..
        if len(j.label.split('/')) > n_levels:
            root_label = ''.join(j.label.split('/')[:n_levels])
            if root_label in job_groups.keys():
                job_groups[root_label].append(j)
            else:
                job_groups[root_label] = [j]
        else:  # otherwise add it into the list as it is..
            root_label = ''.join(j.label.split('/'))
            if root_label in job_groups.keys():
                job_groups[root_label].append(j)
            else:
                job_groups[root_label] = [j]

    print "job grouping done!!"

    print "creating the grouped model jobs.."
    operational_model_jobs = []
    label_cc = 0
    for k in job_groups.keys():
        # if it is just one job, append it
        if len(job_groups[k]) == 1:
            operational_model_jobs.append(job_groups[k][0])
        else:  # group the jobs into one sa only..
            cat_job = concatenate_modeljobs('grouped-job-{}'.format(label_cc), job_groups[k])
            operational_model_jobs.append(cat_job)
            label_cc += 1

    print "grouped model jobs created!"
    # /////////////////////////////////////////////////////////////////////////

    # ///////////// Fill-in job structure for recommendations.. ///////////////
    # get accounting background jobs..
    # ECMWF_acc_data = ingest_data("accounting", "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test_0.csv")
    ecmwf_acc_data = ingest_data("accounting", "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201.csv")
    print "accounting data ingested!"

    # filter 1day of data only..
    timescale_day = (datetime.datetime(2016, 2, 1, 0, 0, 0), datetime.datetime(2016, 2, 2, 0, 0, 0))

    new_joblist = [j for j in ecmwf_acc_data.joblist
                   if timescale_day[0] < datetime.datetime.fromtimestamp(j.time_start) < timescale_day[1]]

    # model all accounting jobs..
    ecmwf_acc_data.joblist = new_joblist
    acc_model_jobs = [j for j in ecmwf_acc_data.model_jobs()]

    # create database with all job records..
    time_signals_types = std_model_jobs[0].timesignals.keys()

    # add the darshan job records
    all_jobs_data_dsh = np.zeros((len(matching_jobs), len(time_signals_types) + 3))
    for cc, j in enumerate(matching_jobs):
        # take only non None metrics in the jobs..
        row = [ts.sum if ts is not None else 0 for tsk, ts in j.timesignals.items()]
        row += [j.ncpus, j.nnodes, j.duration]
        all_jobs_data_dsh[cc, :] = np.asarray(row)

    data_all = all_jobs_data_dsh
    # n_jobs = data_all.shape[0]
    # n_items = data_all.shape[1]
    print "created job metrics structure!"

    print "creating user-item and item-item similarities.."
    # use scikit to split the data into training and teting set..
    train_data_matrix, test_data_matrix = cv.train_test_split(data_all, test_size=0.25)

    # calculate cosine similarities (user-user matrix and item-item matrix)..
    # these similarity matrices will then be used to predict the missing data for "new users" (jobs..)
    user_similarity_matrix = pairwise_distances(train_data_matrix, metric='cosine')
    item_similarity_matrix = pairwise_distances(train_data_matrix.T, metric='cosine')

    print "user-item matrix size = {}".format(user_similarity_matrix.shape)
    print "item-item matrix size = {}".format(item_similarity_matrix.shape)

    # # predict the items metrics of the training set..
    # item_pred = train_data_matrix.dot(item_similarity_matrix) / np.array([np.abs(item_similarity_matrix).sum(axis=1)])
    # print 'Item-based CF RMSE: ' + str(rmse(item_prediction, test_data_matrix))
    # /////////////////////////////////////////////////////////////////////////

    # //////////// Apply recommender system to accounting jobs.. //////////////
    print "Applying recommender system to accounting jobs.."

    # create database with all accounting job records..
    time_signals_types = std_model_jobs[0].timesignals.keys()

    # add the accounting job records
    acc_jobs_data = np.zeros((len(acc_model_jobs), len(time_signals_types) + 3))
    for cc, j in enumerate(acc_model_jobs):
        # take only non None metrics in the jobs..
        row = [ts.sum if ts is not None else 0 for tsk, ts in j.timesignals.items()]
        row += [j.ncpus, j.nnodes, j.duration]
        acc_jobs_data[cc, :] = np.asarray(row)

    # n_jobs = acc_jobs_data.shape[0]
    # n_items = acc_jobs_data.shape[1]

    # use recommender system in accounting logs:
    item_prediction_acc = acc_jobs_data.dot(item_similarity_matrix) / \
                                            np.array([np.abs(item_similarity_matrix).sum(axis=1)])
    item_prediction_acc[:, 9:11] = acc_jobs_data[:, 9:11]
    print "created prediction values for background jobs.."
    # /////////////////////////////////////////////////////////////////////////

    # /// do clustering now that all the jobs have their metrics filled in.. //
    print "doing clustering.."
    rseed = 0
    max_iter = 100
    nc_max = 20
    nc_delta = 1
    nc_vec = np.asarray(range(1, nc_max, nc_delta))

    avg_d_in_clust = np.zeros(nc_vec.shape[0])
    for cc, n_clusters in enumerate(nc_vec):
        print "Doing clustering with {} clusters".format(n_clusters)
        y_pred = KMeans(n_clusters=n_clusters, max_iter=max_iter, random_state=rseed).fit(item_prediction_acc)
        clusters = y_pred.cluster_centers_
        # labels = y_pred.labels_
        pt_to_all_clusters = cdist(item_prediction_acc, clusters, 'euclidean')
        dist_in_c = np.min(pt_to_all_clusters, axis=1)
        avg_d_in_clust[cc] = np.mean(dist_in_c)

    print "clustering done"

    # plot elbow method
    plt.figure()
    plt.title("in clust d_bar (n clust): elbow method ")
    plt.plot(nc_vec, avg_d_in_clust, 'b')
    plt.show()

    # Manually select the number of clusters (this could be stored rather than re-done..)
    y_pred = KMeans(n_clusters=7, max_iter=max_iter, random_state=rseed).fit(item_prediction_acc)
    clusters = y_pred.cluster_centers_
    # labels = y_pred.labels_

    # re-assign recommended values to background jobs..
    print "Re-assigning recommended values.."
    background_model_job_list = []
    for cc, row in enumerate(clusters):
        ts_dict = {}
        for tt, ts_vv in enumerate(row[:8]):
            ts_name = time_signals_types[tt]
            ts = TimeSignal(ts_name).from_values(ts_name, np.arange(10), np.ones(10) * ts_vv)
            ts_dict[ts_name] = ts

        job = ModelJob(
            time_start=0,
            duration=1,
            ncpus=2,
            nnodes=1,
            time_series=ts_dict,
            label="job-{}".format(cc)
        )

        background_model_job_list.append(job)

    print "recommended values reassigned"
    # /////////////////////////////////////////////////////////////////////////

    # /// Create workload from all model jobs (operational and background) ////
    print "Creating  workload from all model jobs (operational and background).."

    # background jobs
    background_sa_list = []
    for cc, job in enumerate(background_model_job_list):
        app = SyntheticApp(
            job_name="RS-appID-{}".format(cc),
            time_signals=job.timesignals,
            ncpus=2,
            nnodes=1,
            time_start=job.time_start
        )

        background_sa_list.append(app)

    operational_sa_list = []
    for cc, job in enumerate(operational_model_jobs):
        app = SyntheticApp(
            job_name="RS-appID-{}".format(cc),
            time_signals=job.timesignals,
            ncpus=2,
            nnodes=1,
            time_start=job.time_start
        )

        operational_sa_list.append(app)

    # create a workload from full list of synthetic apps..
    sa_workload = SyntheticWorkload(config, apps=operational_sa_list + background_sa_list)
    sa_workload.export(10)

    print "workload created and exported!"
    # /////////////////////////////////////////////////////////////////////////


# ///////////////////////////////////////////
if __name__ == '__main__':
    ecmwf_model()
