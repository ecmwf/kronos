#!/usr/bin/env python

import os
import pickle
import numpy as np

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from plugins.plugin_base import PluginBase
# from time_signal import TimeSignal, signal_types
from synthetic_app import SyntheticApp, SyntheticWorkload
from jobs import model_jobs_from_clusters
import data_analysis
from data_analysis import recommender
from kronos_tools.print_colour import print_colour

import job_grouping
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
        self.acc_dataset = None

    def ingest_data(self):
        """
        ingest data (from files or cached pickles..)
        :return:
        """

        super(PluginECMWF, self).ingest_data()

        # retrieve the loaded datasets for further use
        self.darshan_dataset = self.job_datasets[0]
        self.ipm_dataset = self.job_datasets[1]
        self.acc_dataset = self.job_datasets[2]

        self.acc_model_jobs = [j for j in self.acc_dataset.model_jobs()]

    def generate_model(self):
        """
        generate model of workload
        :return:
        """
        print_colour("green", "generating ecmwf model..")

        # ingest data..
        self.ingest_data()

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
                print_colour("white", "job matching iteration: {}".format(cc))

            ipm_job = [ipj for ipj in ipm_model_jobs if
                       (ipj.label + '/serial' == dj.label) or (ipj.label + '/parallel' == dj.label)]

            # merge IPM jobs
            if ipm_job:
                n_match_ipm += 1
                if len(ipm_job) > 1:
                    print_colour("orange", "error, more than 1 matching job found!!")
                else:
                    dj.merge(ipm_job[0], force=True)
                    matching_jobs.append(dj)

        print_colour("white", "matching={} (out of {})".format(n_match_ipm, float(len(dsh_model_jobs))) )
        # /////////////////////////////////////////////////////////////////////////

        # ///////////////// Group operational jobs in the tree ////////////////////
        operational_model_jobs = job_grouping.grouping_by_tree_level(matching_jobs, self.config.plugin)
        # /////////////////////////////////////////////////////////////////////////

        # train recommender system with matched jobs
        recomm_sys = recommender.Recommender()
        recomm_sys.train_model(matching_jobs)

        # apply recommendations to accounting jobs..
        acc_model_jobs_mod = recomm_sys.apply_model_to(self.acc_model_jobs)

        # apply clustering to the accounting jobs
        cluster_handler = data_analysis.factory(self.config.plugin['clustering']['name'], self.config.plugin['clustering'])
        cluster_handler.cluster_jobs(acc_model_jobs_mod)
        clusters_matrix = cluster_handler.clusters
        clusters_labels = cluster_handler.labels

        # calculate real and scaled submittal rate
        total_submit_interval = self.config.plugin['total_submit_interval']
        submit_rate_factor = self.config.plugin['submit_rate_factor']
        submit_times = [j.time_queued for j in self.acc_dataset.joblist]
        real_submit_rate = int((max(submit_times) - min(submit_times)) / len(submit_times))
        n_jobs = int((real_submit_rate * submit_rate_factor) * total_submit_interval)
        start_times_vec = np.random.rand(n_jobs) * total_submit_interval

        # create model jobs from clusters and time rates..
        background_model_job_list = model_jobs_from_clusters(clusters_matrix,
                                                             clusters_labels,
                                                             start_times_vec,
                                                             nprocs=self.config.plugin['sa_n_proc'],
                                                             nnodes=self.config.plugin['sa_n_nodes'])

        # /// Create workload from all model jobs (operational and background) ////
        print_colour("white", "Creating  workload from all model jobs (operational and background)..")

        # background jobs
        background_sa_list = []
        for cc, job in enumerate(background_model_job_list):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=self.config.plugin['sa_n_proc'],
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

        # TODO: use background jobs only for now..
        # sa_workload = SyntheticWorkload(self.config, apps=operational_sa_list + background_sa_list)
        sa_workload = SyntheticWorkload(self.config, background_sa_list)

        sa_workload.set_tuning_factors(self.config.plugin['tuning_factors'])
        sa_workload.export(self.config.plugin['sa_n_frames'])
        sa_workload.save()

    def run(self):
        """
        run model of workload
        :return:
        """
        print_colour("green", "running ecmwf plugin..")
        ecmwf_runner = runner.factory(self.config.runner['type'], self.config)
        ecmwf_runner.run()

    def postprocess(self, postprocess_flag):
        """
        run post-process
        :return:
        """
        print_colour("green", "running ecmwf postprocessing..")
        super(PluginECMWF, self).postprocess(postprocess_flag)

