# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging

import numpy as np
from kronos_executor.definitions import time_signal_names
from kronos_modeller.jobs import ModelJob
from kronos_modeller.time_signal.time_signal import TimeSignal
from kronos_modeller.workload_modelling.job_generation.job_generator import JobGenerator

logger = logging.getLogger(__name__)


class JobGeneratorSpawn(JobGenerator):
    """
    This class generates jobs by spawning them from the cluster centroids
    """

    def __init__(self, schedule_strategy, wl_clusters, config):
        super(JobGeneratorSpawn, self).__init__(schedule_strategy, wl_clusters, config)

    def generate_jobs(self):

        logger.info("Generating jobs from cluster: {}, "
                    "that has {} jobs".format(self.wl_clusters['source-workload'],
                                              len(self.wl_clusters['jobs_for_clustering'])))

        start_times_vec_sa, _, _ = self.schedule_strategy.create_schedule()

        # Random vector of cluster indexes
        n_modelled_jobs = len(start_times_vec_sa)
        np.random.seed(self.config["job_submission_strategy"].get('random_seed', 0))
        vec_clust_indexes = np.random.randint(self.wl_clusters['cluster_matrix'].shape[0],
                                              size=n_modelled_jobs)

        # Mean NCPU in cluster (considering jobs in cluster)
        jobs_all = self.wl_clusters['jobs_for_clustering']
        lab_all = np.asarray(self.wl_clusters['labels'])

        # jobs in each cluster
        jobs_in_each_cluster = {cl: np.asarray(jobs_all)[lab_all == cl] for cl in set(lab_all)}

        # mean #CPUS in each cluster (from jobs for which ncpus is available, otherwise 1)
        mean_cpus = {cl_id: np.mean([job.ncpus if job.ncpus else 1 for job in cl_jobs])
                     for cl_id, cl_jobs in jobs_in_each_cluster.iteritems()}

        # mean #NODES in each cluster (from jobs for which nnodes is available, otherwise 1)
        mean_nodes = {cl_id: np.mean([job.nnodes if job.nnodes else 1 for job in cl_jobs])
                      for cl_id, cl_jobs in jobs_in_each_cluster.iteritems()}

        # loop over the clusters and generates jobs as needed
        generated_model_jobs = []
        for cc, cl_idx in enumerate(vec_clust_indexes):

            ts_dict = {}
            row = self.wl_clusters['cluster_matrix'][cl_idx, :]
            ts_yvalues = np.split(row, len(time_signal_names))
            for tt, ts_vv in enumerate(ts_yvalues):
                ts_name = time_signal_names[tt]
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(len(ts_vv)), ts_vv)
                ts_dict[ts_name] = ts

            job = ModelJob(
                time_start=start_times_vec_sa[cc],
                job_name="job-{}-cl-{}".format(cc, cl_idx),
                duration=None,
                ncpus=mean_cpus[cl_idx],
                nnodes=mean_nodes[cl_idx],
                timesignals=ts_dict,
                label="job-{}-cl-{}".format(cc, cl_idx)
            )
            generated_model_jobs.append(job)

        n_sa = len(generated_model_jobs)
        n_job_ratio = n_sa / float(len(self.wl_clusters['jobs_for_clustering'])) * 100.
        logger.info( "====> Generated {} jobs from cluster (#job ratio = {:.2f}%)".format(n_sa, n_job_ratio))

        return generated_model_jobs, vec_clust_indexes
