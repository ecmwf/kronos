# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging

from kronos_modeller.workload import Workload
from kronos_modeller.workload_modelling.clustering import clustering_types
from kronos_modeller.workload_modelling.job_generation import strategy_factory
from kronos_modeller.workload_modelling.modelling_strategy import WorkloadModellingStrategy
from kronos_modeller.workload_modelling.time_schedule import job_schedule_factory
from kronos_modeller.workload_set import WorkloadSet

logger = logging.getLogger(__name__)


class ClusterSpawnStrategy(WorkloadModellingStrategy):
    """
    Class that apply the following modelling steps to the workload
     - job clustering
     - generates jobs according to a specific time_schedule
    """

    # required configuration keys
    required_config_fields = [
        "type",
        "job_clustering",
        "job_submission_strategy"
    ]

    # map that stores various combinations of generation strategies
    generation_mapping = {
        "match_job_pdf": ("equiv_time_pdf", "spawn"),
        "match_job_pdf_exact": ("equiv_time_pdf_exact", "spawn"),
        "match_job_pdf_exact_rand": ("equiv_time_pdf_exact", "spawn_random")
    }

    def __init__(self, workloads):

        super(ClusterSpawnStrategy, self).__init__(workloads)

        self.clusters = None

    def _apply(self, config):
        """
        Apply the cluster and spawn strategy on the workloads
        :param config:
        :return:
        """

        print("applying {}".format(__name__))
        # configure and apply clustering (as per config)
        classifier = clustering_types[config["job_clustering"]["type"]](self.workloads)
        classifier.apply(config["job_clustering"])
        self.clusters = classifier.get_clusters()
        print("clusters found!")

        # generate the jobs according to the bove clusters and spawning strategy
        self.generate_synthetic_workload(self.clusters, config)

    def generate_synthetic_workload(self, clusters, config):
        """
        Main method that call the specific generation method requested
        :return:
        """

        schedule_key = self.generation_mapping[config["job_submission_strategy"]['type']][0]
        spawning_strategy = self.generation_mapping[config["job_submission_strategy"]['type']][1]

        print("schedule_key ", schedule_key)
        print("spawning_strategy ", spawning_strategy)

        n_bins_for_pdf = config["job_submission_strategy"]["n_bins_for_pdf"]

        global_t0 = min(j.time_start for cl in clusters for j in cl['jobs_for_clustering'])
        global_tend = max(j.time_start for cl in clusters for j in cl['jobs_for_clustering'])

        # generate a synthetic workload for each cluster of jobs
        t_submit_interval = config["job_submission_strategy"]['total_submit_interval']
        submit_rate_factor = config["job_submission_strategy"]['submit_rate_factor']
        for wl_clusters in clusters:
            start_times = [j.time_start for j in wl_clusters['jobs_for_clustering']]
            print("using cluster generated from workload {}".format(wl_clusters["source-workload"]))

            # invoke the required scheduling strategy
            jobs_schedule_strategy = job_schedule_factory[schedule_key](start_times,
                                                                        global_t0,
                                                                        global_tend,
                                                                        t_submit_interval,
                                                                        submit_rate_factor,
                                                                        n_bins_for_pdf)

            # instantiate and invoke the required scheduling strategy
            generation_strategy = strategy_factory[spawning_strategy](jobs_schedule_strategy,
                                                                      wl_clusters,
                                                                      config)

            model_jobs, vec_clust_indexes = generation_strategy.generate_jobs()

            # model jobs
            self.model_jobs = model_jobs
            self.workload_set = WorkloadSet([Workload(self.model_jobs)])

    # @staticmethod
    # def normalize_jobs(model_jobs, wl_clusters):
    #     """
    #     Normalize the time-signals of the generated jobs in order to preserve time-series sums in each sub-workload ---
    #     :param model_jobs:
    #     :param wl_name:
    #     :return:
    #     """
    #
    #     # create a workload from jobs in cluster and generated model jobs
    #     wl_original = WorkloadData(jobs=wl_clusters['jobs_for_clustering'], tag="original_jobs")
    #     wl_generated = WorkloadData(jobs=model_jobs, tag="generated_jobs")
    #     ts_orig = wl_original.total_metrics_sum_dict
    #     ts_generated = wl_generated.total_metrics_sum_dict
    #
    #     # normalize generated jobs
    #     normalized_jobs = copy.deepcopy(model_jobs)
    #     for job in normalized_jobs:
    #         for ts in ts_generated.keys():
    #             if float(ts_generated[ts]):
    #                 job.timesignals[ts].yvalues = job.timesignals[ts].yvalues / float(ts_generated[ts]) * ts_orig[ts]
    #
    #     return normalized_jobs


    #
    #
    # def generate_synthetic_workload(self, clusters, config):
    #     """
    #     Generate synthetic workload form the supplied model jobs
    #     :return:
    #     """
    #
    #     sapps_generator = SyntheticWorkloadGenerator(config, self.clusters)
    #
    #     modelled_sa_jobs = sapps_generator.generate_synthetic_apps()
    #
    #     # # Calculate the radius of gyration of clusters and generated model jobs
    #     # r_gyr_all_pc = {}
    #     # r_gyr_modeljobs_all = sapps_generator.calculate_modeljobs_r_gyrations()
    #     #
    #     # for cluster in self.clusters:
    #     #     r_gyr_wl = np.mean(cluster["r_gyration"])
    #     #     r_gyr_modeljobs = np.mean(r_gyr_modeljobs_all[cluster["source-workload"]])
    #     #     r_gyr_all_pc[cluster["source-workload"]] = np.abs(r_gyr_modeljobs-r_gyr_wl)/r_gyr_wl*100
    #     #
    #     # Report.add_measure(ModelMeasure("r_gyration error [%]", r_gyr_all_pc, __name__))
    #
    #     logger.info( "====> Generating jobs from sub-workload: {}, that has {} jobs".format(self.wl_clusters['source-workload'],
    #                                                                                            len(self.wl_clusters['jobs_for_clustering'])))
    #
    #     start_times_vec_sa, _, _ = self.schedule_strategy.create_schedule()
    #
    #     np.random.seed(self.config['random_seed'])
    #
    #     n_modelled_jobs = len(start_times_vec_sa)
    #     n_clusters = self.wl_clusters['cluster_matrix'].shape[0]
    #     clustered_jobs_all = self.wl_clusters['jobs_for_clustering']
    #     clustered_jobs_labels = self.wl_clusters['labels']
    #
    #     # this is the fraction of jobs that need to be taken from eah cluster..
    #     model_job_fraction = min(1, n_modelled_jobs/float(len(self.wl_clusters['jobs_for_clustering'])))
    #
    #     chosen_model_jobs = []
    #     vec_clust_indexes = np.asarray([], dtype=int)
    #     mean_ncpu_nnodes_in_cluster = []
    #     for ic in range(n_clusters):
    #
    #         # jobs in this cluster
    #         jobs_in_cluster = np.asarray(clustered_jobs_all)[clustered_jobs_labels == ic]
    #         jobs_in_cluster_idxs = np.arange(len(jobs_in_cluster))
    #         n_jobs_to_generate_from_cluster = max(1, int(len(jobs_in_cluster)*model_job_fraction))
    #
    #         if n_jobs_to_generate_from_cluster > len(jobs_in_cluster):
    #             n_jobs_to_generate_from_cluster = len(jobs_in_cluster)
    #
    #         # take a random sample of jobs in cluster (according to job fraction)
    #         jobs_in_cluster_sampled_idx = np.random.choice(jobs_in_cluster_idxs,
    #                                                        n_jobs_to_generate_from_cluster,
    #                                                        replace=False)
    #
    #         jobs_in_cluster_for_generation = []
    #         for idxj in jobs_in_cluster_sampled_idx:
    #             jobs_in_cluster_for_generation.append(jobs_in_cluster[idxj])
    #
    #         # chosen_model_jobs.extend(jobs_in_cluster[jobs_in_cluster_sampled_idx])
    #         chosen_model_jobs.extend(jobs_in_cluster_for_generation)
    #
    #         ncpus_vec = [job.ncpus for job in jobs_in_cluster_for_generation]
    #         ncpus_vec = ncpus_vec if ncpus_vec else [self.config['synthapp_n_cpu']]
    #         mean_ncpus = np.mean(ncpus_vec)
    #
    #         nnodes_vec = [job.nnodes for job in jobs_in_cluster_for_generation]
    #         nnodes_vec = nnodes_vec if nnodes_vec else self.config['synthapp_n_nodes']
    #         mean_nnodes = np.mean(nnodes_vec)
    #         mean_ncpu_nnodes_in_cluster.append({"ncpus": mean_ncpus, "nnodes": mean_nnodes})
    #
    #         logger.info( "cluster {} - mean_ncpus={}, mean_nnodes={}".format(ic, mean_ncpus, mean_nnodes))
    #
    #         vec_clust_indexes = np.append(vec_clust_indexes, np.ones(n_jobs_to_generate_from_cluster, dtype=int)*ic)
    #
    #     # generates model jobs as needed
    #     generated_model_jobs = []
    #     for cc, job in enumerate(chosen_model_jobs):
    #
    #         job_copy = copy.deepcopy(job)
    #
    #         cluster_idx = vec_clust_indexes[cc]
    #
    #         scaled_mean_cpus_in_cluster = mean_ncpu_nnodes_in_cluster[cluster_idx]["ncpus"] * self.config['global_scaling_factor']
    #         job_ncpus = max(1, int(job_copy.ncpus*self.config['global_scaling_factor'])) if job_copy.ncpus else max(1, int(scaled_mean_cpus_in_cluster))
    #
    #         scaled_mean_nnodes_in_cluster = mean_ncpu_nnodes_in_cluster[cluster_idx]["nnodes"] * self.config['global_scaling_factor']
    #         job_nnodes = max(1, int(job_copy.nnodes * self.config['global_scaling_factor'])) if job_copy.nnodes else max(1, int(scaled_mean_nnodes_in_cluster))
    #
    #         # assign this job a start time (if more jobs are created, the start time is chosen randomly within the
    #         # start times..)
    #         if cc < len(start_times_vec_sa):
    #             start_time = start_times_vec_sa[cc]
    #         else:
    #             start_time = start_times_vec_sa[np.random.randint(0, len(start_times_vec_sa), 1)[0]]
    #
    #         job = ModelJob(
    #             time_start=start_time,
    #             duration=None,
    #             ncpus=job_ncpus,
    #             nnodes=job_nnodes,
    #             timesignals=job_copy.timesignals,
    #             label="job-{}".format(cc)
    #         )
    #         generated_model_jobs.append(job)
    #
    #     n_job_ratio = len(generated_model_jobs) / float(len(self.wl_clusters['jobs_for_clustering'])) * 100.
    #     logger.info( "<==== Generated {} jobs (#job ratio = {:.2f}%)".format(len(generated_model_jobs), n_job_ratio))
    #     return generated_model_jobs, vec_clust_indexes.tolist()
