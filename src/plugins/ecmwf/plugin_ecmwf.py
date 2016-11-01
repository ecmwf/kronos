#!/usr/bin/env python

import os
import pickle

import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from plugins.plugin_base import PluginBase
from time_signal import TimeSignal, signal_types
from synthetic_app import SyntheticApp, SyntheticWorkload
from jobs import ModelJob
from kronos_tools.print_colour import print_colour
from elbow_method import find_n_clusters

import logreader
import job_grouping
import recomm_system
import runner


# ////////////////////////////////////////////////////////////////////////////
class PluginECMWF(PluginBase):
    """
    class defining the ecmwf plugin
    """

    def __init__(self, config):
        super(PluginECMWF, self).__init__(config)
        self.name = "ecmwf"
        self.darshan_dataset = None
        self.ipm_dataset = None
        self.stdout_dataset = None
        self.acc_model_jobs = None

    def ingest_data(self):
        """
        ingest data (from files or cached pickles..)
        :return:
        """
        print_colour("green", "ingesting data..")

        # Darshan
        with open(self.config.plugin["darshan_ingested_file"], "r") as f:
            self.darshan_dataset = pickle.load(f)
        print "darshan log data ingested!"

        # IPM
        with open(self.config.plugin["ipm_ingested_file"], "r") as f:
            self.ipm_dataset = pickle.load(f)
        print "ipm log data ingested!"

        # # stdout
        # with open(self.config.plugin["stdout_ingested_file"], "r") as f:
        #     self.stdout_dataset = pickle.load(f)
        # print "stdout log data ingested!"

        # job scheduling data
        acc_dataset = logreader.ingest_data("accounting", self.config.plugin["job_scheduler_logs"])
        acc_dataset.apply_cutoff_dates(self.config.plugin["job_scheduler_date_start"],
                                       self.config.plugin["job_scheduler_date_end"])

        self.acc_model_jobs = [j for j in acc_dataset.model_jobs()]

    def generate_model(self):
        """
        generate model of workload
        :return:
        """

        # ingest data..
        self.ingest_data()

        # generate model..
        print_colour("green", "generating ecmwf model..")

        # the matching will be focused on darshan records..
        # only 6 hours from the start are retained..
        dsh_times = [job.time_start for job in self.darshan_dataset.joblist]
        dsh_t0 = min(dsh_times)
        self.darshan_dataset.joblist = [job for job in self.darshan_dataset.joblist if job.time_start < (dsh_t0 + 6.0 * 3600.0)]

        # model all jobs
        dsh_model_jobs = [j for j in self.darshan_dataset.model_jobs()]
        ipm_model_jobs = [j for j in self.ipm_dataset.model_jobs()]

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
        operational_model_jobs = job_grouping.grouping_by_tree_level(matching_jobs, self.config.plugin)
        # /////////////////////////////////////////////////////////////////////////

        # ////////////// create database with all job records.. ///////////////////
        item_prediction_acc = recomm_system.apply_recomm_sys(matching_jobs, self.acc_model_jobs)
        # /////////////////////////////////////////////////////////////////////////

        # /// do clustering now that all the jobs have their metrics filled in.. //
        print "doing clustering.."

        rseed = self.config.plugin['km_rseed']
        max_iter = self.config.plugin['km_max_iter']
        nc_max = self.config.plugin['km_nc_max']
        nc_delta = self.config.plugin['km_nc_delta']

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

        # Calculate best number of clusters
        n_clusters_optimal = find_n_clusters(avg_d_in_clust)
        y_pred = KMeans(n_clusters=n_clusters_optimal, max_iter=max_iter, random_state=rseed).fit(item_prediction_acc)
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
                ncpus=self.config.plugin['sa_n_proc'],
                nnodes=self.config.plugin['sa_n_nodes'],
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
                ncpus=self.config.plugin['sa_n_proc'],  # TODO: ncpus and nnodes should be different for different apps
                nnodes=self.config.plugin['sa_n_nodes'],
                time_start=job.time_start
            )

            background_sa_list.append(app)

        operational_sa_list = []
        for cc, job in enumerate(operational_model_jobs):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=self.config.plugin['sa_n_proc'],
                nnodes=self.config.plugin['sa_n_nodes'],
                time_start=job.time_start
            )

            operational_sa_list.append(app)

        # create a workload from full list of synthetic apps..
        sa_workload = SyntheticWorkload(self.config, apps=operational_sa_list + background_sa_list)
        sa_workload.set_tuning_factors(self.config.plugin['tuning_factors'])
        sa_workload.export(self.config.plugin['sa_n_frames'])
        sa_workload.save()

        print "workload created and exported!"
        print sa_workload.total_metrics_dict(include_tuning_factors=True)

    def run(self):
        """
        run model of workload
        :return:
        """
        print_colour("green", "running ecmwf plugin..")
        ecmwf_runner = runner.factory(self.config.runner['type'], self.config)
        ecmwf_runner.run()
        ecmwf_runner.plot_results()

    def postprocess(self):
        """
        postprocess run
        :return:
        """
        print_colour("green", "doing ecmwf postprocessing..")
        super(PluginECMWF, self).postprocess()
