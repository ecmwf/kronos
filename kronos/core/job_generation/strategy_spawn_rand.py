# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
import copy

from kronos.core.kronos_tools.print_colour import print_colour
from kronos.core.job_generation.strategy_base import StrategyBase
from kronos.core.jobs import ModelJob

class StrategySpawnRand(StrategyBase):
    """
    This class generates jobs by spawning them from the jobs taken randomly among the clusters
    """

    def __init__(self, schedule_strategy, wl_clusters, config):
        super(StrategySpawnRand, self).__init__(schedule_strategy, wl_clusters, config)

    def generate_jobs(self):

        print_colour("white", "Generating jobs from cluster: {}, that has {} jobs".format(self.wl_clusters['source-workload'],
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
        vec_clust_indexes = np.asarray([],dtype=int)
        for ic in range(n_clusters):

            # jobs in this cluster
            jobs_in_cluster = np.asarray(clustered_jobs_all)[clustered_jobs_labels == ic]
            jobs_in_cluster_idxs = np.arange(len(jobs_in_cluster))
            n_jobs_to_generate_from_cluster = max(1, int(len(jobs_in_cluster)*model_job_fraction))

            # take a random sample of jobs in cluster (according to job fraction)
            jobs_in_cluster_sampled_idx = np.random.choice(jobs_in_cluster_idxs,
                                                           n_jobs_to_generate_from_cluster,
                                                           replace=False)

            chosen_model_jobs.extend(jobs_in_cluster[jobs_in_cluster_sampled_idx])
            vec_clust_indexes = np.append(vec_clust_indexes, np.ones(n_jobs_to_generate_from_cluster, dtype=int)*ic)

        # make sure that the total model jobs produced do not exceeded the global number decided in the schedule
        if len(chosen_model_jobs) > len(start_times_vec_sa):
            chosen_model_jobs = chosen_model_jobs[:len(start_times_vec_sa)]
            vec_clust_indexes = vec_clust_indexes[:len(start_times_vec_sa)]

        # generates model jobs as needed
        generated_model_jobs = []
        for cc, job in enumerate(chosen_model_jobs):

            job_copy = copy.deepcopy(job)

            job = ModelJob(
                time_start=start_times_vec_sa[cc],
                duration=None,
                ncpus=self.config['synthapp_n_cpu'],
                nnodes=self.config['synthapp_n_nodes'],
                timesignals=job_copy.timesignals,
                label="job-{}".format(cc)
            )
            generated_model_jobs.append(job)

        n_sa = len(generated_model_jobs)
        n_job_ratio = n_sa / float(len(self.wl_clusters['jobs_for_clustering'])) * 100.
        print_colour("white", "====> Generated {} jobs from cluster (#job ratio = {:.2f}%)".format(n_sa, n_job_ratio))

        return generated_model_jobs, vec_clust_indexes.tolist()
