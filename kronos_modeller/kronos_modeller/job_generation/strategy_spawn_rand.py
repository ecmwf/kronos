# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import copy

import numpy as np

from kronos_modeller.job_generation.strategy_base import StrategyBase
from kronos_modeller.jobs import ModelJob
from kronos_executor.tools import print_colour


class StrategySpawnRand(StrategyBase):
    """
    This class generates jobs by spawning them from the jobs taken randomly among the clusters
    """

    def __init__(self, schedule_strategy, wl_clusters, config):
        super(StrategySpawnRand, self).__init__(schedule_strategy, wl_clusters, config)

    def generate_jobs(self):

        print_colour("white", "====> Generating jobs from sub-workload: {}, that has {} jobs".format(self.wl_clusters['source-workload'],
                                                                                               len(self.wl_clusters['jobs_for_clustering'])))

        start_times_vec_sa, _, _ = self.schedule_strategy.create_schedule()

        np.random.seed(self.config['random_seed'])

        n_modelled_jobs = len(start_times_vec_sa)
        n_clusters = self.wl_clusters['cluster_matrix'].shape[0]
        clustered_jobs_all = self.wl_clusters['jobs_for_clustering']
        clustered_jobs_labels = self.wl_clusters['labels']

        # this is the fraction of jobs that need to be taken from eah cluster..
        model_job_fraction = min(1, n_modelled_jobs/float(len(self.wl_clusters['jobs_for_clustering'])))

        chosen_model_jobs = []
        vec_clust_indexes = np.asarray([], dtype=int)
        mean_ncpu_nnodes_in_cluster = []
        for ic in range(n_clusters):

            # jobs in this cluster
            jobs_in_cluster = np.asarray(clustered_jobs_all)[clustered_jobs_labels == ic]
            jobs_in_cluster_idxs = np.arange(len(jobs_in_cluster))
            n_jobs_to_generate_from_cluster = max(1, int(len(jobs_in_cluster)*model_job_fraction))

            if n_jobs_to_generate_from_cluster > len(jobs_in_cluster):
                n_jobs_to_generate_from_cluster = len(jobs_in_cluster)

            # take a random sample of jobs in cluster (according to job fraction)
            jobs_in_cluster_sampled_idx = np.random.choice(jobs_in_cluster_idxs,
                                                           n_jobs_to_generate_from_cluster,
                                                           replace=False)

            jobs_in_cluster_for_generation = []
            for idxj in jobs_in_cluster_sampled_idx:
                jobs_in_cluster_for_generation.append(jobs_in_cluster[idxj])

            # chosen_model_jobs.extend(jobs_in_cluster[jobs_in_cluster_sampled_idx])
            chosen_model_jobs.extend(jobs_in_cluster_for_generation)

            ncpus_vec = [job.ncpus for job in jobs_in_cluster_for_generation]
            ncpus_vec = ncpus_vec if ncpus_vec else [self.config['synthapp_n_cpu']]
            mean_ncpus = np.mean(ncpus_vec)

            nnodes_vec = [job.nnodes for job in jobs_in_cluster_for_generation]
            nnodes_vec = nnodes_vec if nnodes_vec else self.config['synthapp_n_nodes']
            mean_nnodes = np.mean(nnodes_vec)
            mean_ncpu_nnodes_in_cluster.append({"ncpus": mean_ncpus, "nnodes": mean_nnodes})

            print_colour("white", "cluster {} - mean_ncpus={}, mean_nnodes={}".format(ic, mean_ncpus, mean_nnodes))

            vec_clust_indexes = np.append(vec_clust_indexes, np.ones(n_jobs_to_generate_from_cluster, dtype=int)*ic)

        # generates model jobs as needed
        generated_model_jobs = []
        for cc, job in enumerate(chosen_model_jobs):

            job_copy = copy.deepcopy(job)

            cluster_idx = vec_clust_indexes[cc]

            scaled_mean_cpus_in_cluster = mean_ncpu_nnodes_in_cluster[cluster_idx]["ncpus"] * self.config['global_scaling_factor']
            job_ncpus = max(1, int(job_copy.ncpus*self.config['global_scaling_factor'])) if job_copy.ncpus else max(1, int(scaled_mean_cpus_in_cluster))

            scaled_mean_nnodes_in_cluster = mean_ncpu_nnodes_in_cluster[cluster_idx]["nnodes"] * self.config['global_scaling_factor']
            job_nnodes = max(1, int(job_copy.nnodes * self.config['global_scaling_factor'])) if job_copy.nnodes else max(1, int(scaled_mean_nnodes_in_cluster))

            # assign this job a start time (if more jobs are created, the start time is chosen randomly within the
            # start times..)
            if cc < len(start_times_vec_sa):
                start_time = start_times_vec_sa[cc]
            else:
                start_time = start_times_vec_sa[np.random.randint(0, len(start_times_vec_sa), 1)[0]]

            job = ModelJob(
                time_start=start_time,
                duration=None,
                ncpus=job_ncpus,
                nnodes=job_nnodes,
                timesignals=job_copy.timesignals,
                label="job-{}".format(cc)
            )
            generated_model_jobs.append(job)

        n_job_ratio = len(generated_model_jobs) / float(len(self.wl_clusters['jobs_for_clustering'])) * 100.
        print_colour("white", "<==== Generated {} jobs (#job ratio = {:.2f}%)".format(len(generated_model_jobs), n_job_ratio))
        return generated_model_jobs, vec_clust_indexes.tolist()
