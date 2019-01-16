# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import numpy as np
from kronos_executor.tools import print_colour
from kronos_executor.definitions import time_signal_names
from kronos_modeller.job_generation.strategy_base import StrategyBase
from kronos_modeller.jobs import ModelJob
from kronos_modeller.time_signal.time_signal import TimeSignal


class StrategySpawn(StrategyBase):
    """
    This class generates jobs by spawning them from the cluster centroids
    """

    def __init__(self, schedule_strategy, wl_clusters, config):
        super(StrategySpawn, self).__init__(schedule_strategy, wl_clusters, config)

    def generate_jobs(self):

        print_colour("white", "Generating jobs from cluster: {}, that has {} jobs".format(self.wl_clusters['source-workload'],
                                                                          len(self.wl_clusters['jobs_for_clustering'])))

        start_times_vec_sa, _, _ = self.schedule_strategy.create_schedule()

        # Random vector of cluster indices
        n_modelled_jobs = len(start_times_vec_sa)
        np.random.seed(self.config['random_seed'])
        vec_clust_indexes = np.random.randint(self.wl_clusters['cluster_matrix'].shape[0], size=n_modelled_jobs)

        # loop over the clusters and generates jos as needed
        generated_model_jobs = []
        for cc, idx in enumerate(vec_clust_indexes):

            ts_dict = {}
            row = self.wl_clusters['cluster_matrix'][idx, :]
            ts_yvalues = np.split(row, len(time_signal_names))
            for tt, ts_vv in enumerate(ts_yvalues):
                ts_name = time_signal_names[tt]
                ts = TimeSignal(ts_name).from_values(ts_name, np.arange(len(ts_vv)), ts_vv)
                ts_dict[ts_name] = ts

            job = ModelJob(
                time_start=start_times_vec_sa[cc],
                duration=None,
                ncpus=self.config['synthapp_n_cpu'],
                nnodes=self.config['synthapp_n_nodes'],
                timesignals=ts_dict,
                label="job-{}-cl-{}".format(cc, idx)
            )
            generated_model_jobs.append(job)

        n_sa = len(generated_model_jobs)
        n_job_ratio = n_sa / float(len(self.wl_clusters['jobs_for_clustering'])) * 100.
        print_colour("white", "====> Generated {} jobs from cluster (#job ratio = {:.2f}%)".format(n_sa, n_job_ratio))

        return generated_model_jobs, vec_clust_indexes
