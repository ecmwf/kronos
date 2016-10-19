#!/usr/bin/env python

import os

import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from plugins.plugin_base import PluginBase
from time_signal import TimeSignal, signal_types
from synthetic_app import SyntheticApp, SyntheticWorkload
from config.config import Config
from jobs import ModelJob

import helper_functions
import job_grouping
import recomm_system


# Load config
config = Config()


# ////////////////////////////////////////////////////////////////////////////
class PluginECMWF(PluginBase):
    """
    class defining the ecmwf plugin
    """

    def run(self):

        # some raw settings for this script..
        settings_dict = {
                         'n_tree_levels': 3,
                         'km_rseed': 0,
                         'km_max_iter': 100,
                         'km_nc_max': 20,
                         'km_nc_delta': 1,
                         'sa_n_proc': 2,
                         'sa_n_nodes': 1,
                         'path_ingested_jobs': '/perm/ma/maab/ngio_ingested_data/my_ingested'
                        }

        # ingest all datasets..
        dsh_dataset, \
        ipm_dataset, \
        std_dataset = helper_functions.ingest_operational_logs(settings_dict['path_ingested_jobs'])

        # /////////////// matching between darshan, stdout and ipm ////////////////
        print "matching corresponding jobs.."

        # the matching will be focused on darshan records..
        # only 6 hours from the start are retained..
        dsh_times = [job.time_start for job in dsh_dataset.joblist]
        dsh_t0 = min(dsh_times)
        dsh_dataset.joblist = [job for job in dsh_dataset.joblist if job.time_start < (dsh_t0 + 6.0 * 3600.0)]

        # model all jobs
        dsh_model_jobs = [j for j in dsh_dataset.model_jobs()]
        ipm_model_jobs = [j for j in ipm_dataset.model_jobs()]

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
                    dj.merge(ipm_job[0], force=True)
                    matching_jobs.append(dj)

        print "matching={} (out of {})".format(n_match_ipm, float(len(dsh_model_jobs)))
        # /////////////////////////////////////////////////////////////////////////

        # ///////////////// Group operational jobs in the tree ////////////////////
        operational_model_jobs = job_grouping.grouping_by_tree_level(matching_jobs, settings_dict)
        # /////////////////////////////////////////////////////////////////////////

        # /////////////////////// Read in accounting jobs.. ///////////////////////
        acc_model_jobs = helper_functions.read_accounting_jobs('/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201.csv')
        # /////////////////////////////////////////////////////////////////////////

        # ////////////// create database with all job records.. ///////////////////
        item_prediction_acc = recomm_system.apply_recomm_sys(matching_jobs, acc_model_jobs)
        # /////////////////////////////////////////////////////////////////////////

        # /// do clustering now that all the jobs have their metrics filled in.. //
        print "doing clustering.."

        rseed = settings_dict['km_rseed']
        max_iter = settings_dict['km_max_iter']
        nc_max = settings_dict['km_nc_max']
        nc_delta = settings_dict['km_nc_delta']

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
                ts_name = signal_types.keys()[tt]
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(10), np.ones(10) * ts_vv)
                ts_dict[ts_name] = ts

            job = ModelJob(
                time_start=0,
                duration=1,
                ncpus=settings_dict['sa_n_proc'],
                nnodes=settings_dict['sa_n_nodes'],
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
                ncpus=settings_dict['sa_n_proc'], # TODO: ncpus and nnodes should be different for different apps..
                nnodes=settings_dict['sa_n_nodes'],
                time_start=job.time_start
            )

            background_sa_list.append(app)

        operational_sa_list = []
        for cc, job in enumerate(operational_model_jobs):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=settings_dict['sa_n_proc'],
                nnodes=settings_dict['sa_n_nodes'],
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

    plg = PluginECMWF()
    plg.run()

