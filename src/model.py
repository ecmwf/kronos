import os
import numpy as np

import data_analysis
import workload_data
from exceptions_iows import ConfigurationError

from jobs import model_jobs_from_clusters
from synthetic_app import SyntheticApp, SyntheticWorkload
from kronos_tools.print_colour import print_colour
from config.config import Config


class KronosModel(object):
    """
    The model class operates on a list of workloads:
    1) applies corrections to the workloads according to the configuration instructions passed by the user
    2) applies a modelling strategy
    3) returns a synthetic workload
    """

    def __init__(self, workloads, config):

        assert all(isinstance(wl, workload_data.WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        # configuration parameters
        self.config = config
        self.fill_in = None
        self.classification = None
        self.generator = None

        self.workloads = workloads
        self.jobs_for_clustering = []
        self.clusters = []
        self.modelled_sa_jobs = []

        # check all the configurations
        for k, v in self.config.model.iteritems():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

    def generate_model(self):
        """
        takes the list of workloads and applies user instructions to generate a model
        :return:
        """

        # 1) apply fill-in strategies
        self._apply_workload_fillin()

        # 2) apply classification
        self._apply_classification()

        # 3) apply generation
        self._apply_generation()

    def _apply_workload_fillin(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """

        # call functions corresponding to fill_in types
        for fillin_function in self.fill_in:
            getattr(self, fillin_function['type'])()

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model
        :return:
        """

        if not self.modelled_sa_jobs:
            raise ConfigurationError("cannot export before generating the model!")

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)
        sa_workload.set_tuning_factors(self.generator['tuning_factors'])

        # export the synthetic workload
        ksf_path = os.path.join(self.config.dir_output, self.config.ksf_filename)
        sa_workload.export_ksf(ksf_path, self.generator['sa_n_frames'])

    def postprocess(self):
        """
        Postprocess the results of the run
        :return:
        """
        pass

    def fill_missing_entries(self):
        """
        Apply default values if specified
        :return:
        """

        default_list = [entry for entry in self.fill_in if entry['type'] == "fill_missing_entries"]

        for i_def in default_list:
            for wl in self.workloads:
                if wl.tag in i_def['apply_to']:
                    wl.apply_default_metrics(i_def['metrics'])

    def match_by_keyword(self):
        """
        Apply a lookup table to add metrics from jobs in a workload to another
        :return:
        """

        match_list = [entry for entry in self.fill_in if entry['type'] == "match_by_keyword"]

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        for i_match in match_list:
            for wl_source_tag in i_match['source_workloads']:
                wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
                for wl_dest_tag in i_match['apply_to']:
                    wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                    n_destination_jobs += len(wl_dest.jobs)
                    n_job_matched += wl_dest.apply_lookup_table(wl_source, i_match['similarity_threshold'])

        print_colour("white", "jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))

    def recommender_system(self):
        """
        This implements the recommender system corrections
        :return:
        """
        print "TO IMPLEMENT RECOMMENDER SYSTEM"

    def _check_jobs(self):
        """
        Check that all jobs have the minimum required fields..
        :return:
        """
        for wl in self.workloads:
            for job in wl.jobs:
                job.check_job()

    def _apply_clustering(self, jobs):
        """
        Apply clustering on the selected workloads
        :return:
        """

        # apply clustering to the accounting jobs
        cluster_handler = data_analysis.factory(self.classification['clustering']['type'],
                                                self.classification['clustering'])

        cluster_handler.cluster_jobs(jobs)
        clusters_matrix = cluster_handler.clusters
        clusters_labels = cluster_handler.labels

        return clusters_matrix, clusters_labels

    def _apply_classification(self):
        """
        Apply modelling classification to selected workloads
        :return:
        """

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        self._check_jobs()

        # loop over all the workloads used to create the synthetic workload
        wl_jobs = []
        for wl_entry in self.classification['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_entry)
            wl_jobs.extend(wl.jobs)

            # Apply clustering
            clusters_matrix, clusters_labels = self._apply_clustering(wl_jobs)

            self.clusters.append({
                                  'source-workload': wl_entry,
                                  'jobs_for_clustering': wl_jobs,
                                  'cluster_matrix': clusters_matrix,
                                  'labels': clusters_labels,
                                  })

    def _apply_generation(self):
        """
        Generate synthetic workload form the supplied model jobs
        :return:
        """

        # generate a synthetic workload for each cluster of jobs
        for cluster in self.clusters:

            # calculate the submit rate from the selected workload
            submit_times = [j.time_queued for j in cluster['jobs_for_clustering']]
            real_submit_rate = float(len(submit_times)) / (max(submit_times) - min(submit_times))
            requested_submit_rate = real_submit_rate * self.generator['submit_rate_factor']
            n_modelled_jobs = int(requested_submit_rate * self.generator['total_submit_interval'])

            if not n_modelled_jobs:
                print_colour("orange", "Low submit rate (real={} jobs/sec, requested={} jobs/sec), => number of jobs={}"
                             .format(real_submit_rate, requested_submit_rate, n_modelled_jobs))

                # set n jobs to one! (the very minimum)
                n_modelled_jobs = 1

            start_times_vec = np.random.rand(n_modelled_jobs) * self.generator['total_submit_interval']

            print "real_submit_rate", real_submit_rate
            print "n_jobs", n_modelled_jobs

            # create model jobs from clusters and time rates..
            modelled_jobs = model_jobs_from_clusters(cluster['cluster_matrix'],
                                                     start_times_vec,
                                                     nprocs=self.generator['sa_n_proc'],
                                                     nnodes=self.generator['sa_n_nodes']
                                                     )

            # create synthetic workload from modelled jobs
            # TODO: append workload tag to the synthetic app here!
            modelled_sa_jobs = []
            for cc, job in enumerate(modelled_jobs):
                app = SyntheticApp(
                    job_name="RS-appID-{}".format(cc),
                    time_signals=job.timesignals,
                    ncpus=self.generator['sa_n_proc'],
                    nnodes=self.generator['sa_n_nodes'],
                    time_start=job.time_start
                )

                modelled_sa_jobs.append(app)

            self.modelled_sa_jobs.extend(modelled_sa_jobs)

