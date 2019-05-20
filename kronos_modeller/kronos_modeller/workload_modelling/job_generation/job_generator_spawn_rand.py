# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import copy
import logging

import numpy as np
from kronos_modeller.jobs import ModelJob
from kronos_modeller.workload_modelling.job_generation.job_generator import JobGenerator

logger = logging.getLogger(__name__)


class JobGeneratorSpawnRand(JobGenerator):
    """
    This class generates jobs by spawning them from the jobs taken randomly among the clusters
    """

    def __init__(self, schedule_strategy, wl_clusters, config):
        super(JobGeneratorSpawnRand, self).__init__(schedule_strategy, wl_clusters, config)

    def generate_jobs(self):

        logger.info("====> Generating jobs from sub-workload: {}, "
                    "that has {} jobs".format(self.wl_clusters['source-workload'],
                                              len(self.wl_clusters['jobs_for_clustering'])))

        start_times_vec_sa, _, _ = self.schedule_strategy.create_schedule()

        np.random.seed(self.config["job_submission_strategy"]['random_seed'])

        n_modelled_jobs = len(start_times_vec_sa)
        n_clusters = self.wl_clusters['cluster_matrix'].shape[0]
        clustered_jobs_all = self.wl_clusters['jobs_for_clustering']
        clustered_jobs_labels = self.wl_clusters['labels']

        # this is the fraction of jobs that need to be taken from eah cluster..
        n_job_cluster = len(self.wl_clusters['jobs_for_clustering'])
        model_job_fraction = min(1.0, n_modelled_jobs/float(n_job_cluster))

        chosen_model_jobs = []
        vec_clust_indexes = np.asarray([], dtype=int)
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

            vec_clust_indexes = np.append(vec_clust_indexes,
                                          np.ones(n_jobs_to_generate_from_cluster, dtype=int)*ic)

        # generates model jobs as needed
        generated_model_jobs = []
        for cc, job in enumerate(chosen_model_jobs):

            job_copy = copy.deepcopy(job)

            # assign this job a start time (if more jobs are created,
            # the start time is chosen randomly within the start times..)
            if cc < len(start_times_vec_sa):
                start_time = start_times_vec_sa[cc]
            else:
                start_time = start_times_vec_sa[np.random.randint(0, len(start_times_vec_sa), 1)[0]]

            # choose a number of cpu for this job
            if not job.ncpus:
                logger.warning("job ID {} had not ncpu specified, "
                               "set to default ncpu=1 instead".format(cc))
                job_ncpus = 1
            else:
                job_ncpus = job.ncpus

            # choose a number of nodes for this job
            if not job.nnodes:
                job_nnodes = 1
            else:
                job_nnodes = job.nnodes

            # Spawn and append the model job
            job = ModelJob(
                time_start=start_time,
                duration=None,
                ncpus=job_ncpus,
                nnodes=job_nnodes,
                timesignals=job_copy.timesignals,
                label="job-{}".format(cc)
            )
            generated_model_jobs.append(job)

        n_cluster_jobs = len(self.wl_clusters['jobs_for_clustering'])
        n_job_ratio = len(generated_model_jobs)/float(n_cluster_jobs) * 100.

        logger.info("<==== Generated {} jobs (#job ratio = {:.2f}%)".format(
            len(generated_model_jobs), n_job_ratio))

        return generated_model_jobs, vec_clust_indexes.tolist()
