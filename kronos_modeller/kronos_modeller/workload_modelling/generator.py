# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import copy

import numpy as np

from kronos_modeller.kronos_tools.gyration_radius import r_gyration
from kronos_modeller.synthetic_app import SyntheticApp
from kronos_modeller.workload import Workload
from kronos_modeller.workload_modelling.job_generation import strategy_factory
from kronos_modeller.workload_modelling.time_schedule import job_schedule_factory


class SyntheticWorkloadGenerator(object):
    """
    This class represents a generator thaat reads the output of the clusters and
    produces a synthetic workload according to matching strategies:
    - random: jobs are randomly generated from the clusters to match the submit rate
    - match probability: jobs are generated to match PDF of jobs..
    """

    # this map stores various combinations of generation strategies
    generation_mapping = {
        "match_job_pdf": ("equiv_time_pdf", "spawn"),
        "match_job_pdf_exact": ("equiv_time_pdf_exact", "spawn"),
        "match_job_pdf_exact_rand": ("equiv_time_pdf_exact", "spawn_random")
    }

    def __init__(self, config, clusters):

        self.config = config
        self.clusters = clusters

        # some auxiliary properties of the workloads
        global_t0 = min(j.time_start for cl in clusters for j in cl['jobs_for_clustering'])
        global_tend = max(j.time_start for cl in clusters for j in cl['jobs_for_clustering'])
        n_bins_for_pdf = config["job_submission_strategy"]['n_bins_for_pdf']
        n_bins_timesignals = config["job_clustering"]['num_timesignal_bins']

        self.global_t0 = global_t0
        self.global_tend = global_tend
        self.n_bins_for_pdf = n_bins_for_pdf
        self.n_bins_timesignals = n_bins_timesignals

        # dictionary of all the un-normalized jobs created
        # from clusters (stored for calculating r_gyration)
        self.unnormalized_modelled_jobs_dict = {}

    def generate_synthetic_apps(self):
        """
        Main method that call the specific generation method requested
        :return:
        """

        schedule_key = self.generation_mapping[self.config["job_submission_strategy"]['type']][0]
        spawning_strategy = self.generation_mapping[self.config["job_submission_strategy"]['type']][1]

        generated_sa_from_all_wl = []
        n_bins_for_pdf = self.n_bins_for_pdf if self.n_bins_for_pdf else 20

        # generate a synthetic workload for each cluster of jobs
        t_submit_interval = self.config["job_submission_strategy"]['total_submit_interval']
        submit_rate_factor = self.config["job_submission_strategy"]['submit_rate_factor']
        for wl_clusters in self.clusters:
            start_times = [j.time_start for j in wl_clusters['jobs_for_clustering']]
            print("using cluster generated from workload {}".format(wl_clusters["source-workload"]))

            # invoke the required scheduling strategy
            jobs_schedule_strategy = job_schedule_factory[schedule_key](start_times,
                                                                        self.global_t0,
                                                                        self.global_tend,
                                                                        t_submit_interval,
                                                                        submit_rate_factor,
                                                                        n_bins_for_pdf)

            # instantiate and invoke the required scheduling strategy
            generation_strategy = strategy_factory[spawning_strategy](jobs_schedule_strategy,
                                                                      wl_clusters,
                                                                      self.config)

            model_jobs, vec_clust_indexes = generation_strategy.generate_jobs()

            # Store the model jobs into the unnormalized_modelled_jobs_dict
            self.unnormalized_modelled_jobs_dict[wl_clusters['source-workload']] = (model_jobs, vec_clust_indexes)

            normalized_jobs = self.normalize_jobs(model_jobs, wl_clusters)

            # Then create the synthetic apps from the generated model jobs
            modelled_sa_jobs = self.model_jobs_to_sa(normalized_jobs,
                                                     wl_clusters['source-workload'],
                                                     metrics_hard_limits=self.config.get("metrics_hard_limits"))


    @staticmethod
    def normalize_jobs(model_jobs, wl_clusters):
        """
        Normalize the time-signals of the generated jobs in order to preserve time-series sums in each sub-workload ---
        :param model_jobs:
        :param wl_name:
        :return:
        """

        # create a workload from jobs in cluster and generated model jobs
        wl_original = Workload(jobs=wl_clusters['jobs_for_clustering'], tag="original_jobs")
        wl_generated = Workload(jobs=model_jobs, tag="generated_jobs")
        ts_orig = wl_original.total_metrics_sum_dict
        ts_generated = wl_generated.total_metrics_sum_dict

        # normalize generated jobs
        normalized_jobs = copy.deepcopy(model_jobs)
        for job in normalized_jobs:
            for ts in ts_generated.keys():
                if float(ts_generated[ts]):
                    job.timesignals[ts].yvalues = job.timesignals[ts].yvalues / float(ts_generated[ts]) * ts_orig[ts]

        return normalized_jobs

    @staticmethod
    def model_jobs_to_sa(model_jobs, label, metrics_hard_limits=None):
        """
        This method takes a list of model jobs and translates tham into synthetic apps
        :param model_jobs:
        :param label:
        :param metrics_hard_limits
        :return:
        """

        sa_list = []
        for cc, job in enumerate(model_jobs):
            app = SyntheticApp(
                job_name="RS-appID-{}".format(cc),
                time_signals=job.timesignals,
                ncpus=job.ncpus,
                time_start=job.time_start,
                metrics_hard_limits=metrics_hard_limits,
                label=label
            )

            sa_list.append(app)

        return sa_list

    def calculate_modeljobs_r_gyrations(self):

        # take the number of bins in the cluster data
        nbins = self.n_bins_timesignals
        r_gyr_wl_all = {wl_name: [] for wl_name in self.unnormalized_modelled_jobs_dict.keys()}

        for wl_name in self.unnormalized_modelled_jobs_dict.keys():

            jobs, clustidx = self.unnormalized_modelled_jobs_dict[wl_name]

            jobs = np.asarray(jobs)
            clustidx = np.asarray(clustidx)

            # loop over all the cluster indices of this wl
            for idx in range(max(clustidx)+1):

                # create a matrix of values from jobs associated to this cluster
                jobs_from_cluster = jobs[clustidx == idx]

                if jobs_from_cluster.size:
                    matrix_jobs_in_cluster = np.asarray([j.ts_to_vector(nbins) for j in jobs_from_cluster])

                    # Calculate the radius of gyration for this cluster
                    r_gyr_wl_all[wl_name].append(r_gyration(matrix_jobs_in_cluster))

        return r_gyr_wl_all
