import os
import numpy as np

import data_analysis
import workload_data
from exceptions_iows import ConfigurationError

from jobs import model_jobs_from_clusters
from synthetic_app import SyntheticApp, SyntheticWorkload
from kronos_tools.print_colour import print_colour
from config.config import Config
from workload_fill_in import WorkloadFiller


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
        self.config_fill_in = None
        self.config_classification = None
        self.config_generator = None
        self.config_functions = None

        self.workloads = workloads
        self.jobs_for_clustering = []
        self.clusters = []
        self.modelled_sa_jobs = []

        # check all the configurations
        for k, v in self.config.model.iteritems():
            if not hasattr(self, 'config_'+k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, 'config_'+k, v)

    def generate_model(self):
        """
        takes the list of workloads and performs three actions in sequence:
        1) applies user instructions on potentially partially filled workloads
        2) generate a workload model through clustering+generation process
        3) export the workload as a synthetic workload
        :return:
        """

        # 1) apply fill-in strategies
        self._apply_workload_fillin()

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        self._check_jobs()

        # 2) apply classification
        self._apply_classification()

        # 3) apply generation
        self._apply_generation()

    def _apply_workload_fillin(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """

        filler = WorkloadFiller(self.config_fill_in, self.config_functions, self.workloads)

        # call functions corresponding to fill_in types
        for fillin_function in self.config_fill_in:
            getattr(filler, fillin_function['type'])()

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model
        :return:
        """

        if not self.modelled_sa_jobs:
            raise ConfigurationError("cannot export before generating the model!")

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)
        sa_workload.set_tuning_factors(self.config_generator['tuning_factors'])

        # export the synthetic workload
        ksf_path = os.path.join(self.config.dir_output, self.config.ksf_filename)
        sa_workload.export_ksf(ksf_path, self.config_generator['sa_n_frames'])

    def _check_jobs(self):
        """
        Check that all jobs have the minimum required fields..
        :return:
        """
        print_colour("green", "Checking all jobs before classification..")

        # check only the jobs to be used for classification..
        for wl_name in self.config_classification['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_name)
            wl.check_jobs()

    def _apply_clustering(self, jobs):
        """
        Apply clustering on the selected workloads
        :return:
        """

        # apply clustering to the accounting jobs
        cluster_handler = data_analysis.factory(self.config_classification['clustering']['type'],
                                                self.config_classification['clustering'])

        cluster_handler.cluster_jobs(jobs)
        clusters_matrix = cluster_handler.clusters
        clusters_labels = cluster_handler.labels

        return clusters_matrix, clusters_labels

    def _apply_classification(self):
        """
        Apply modelling classification to selected workloads
        :return:
        """

        # loop over all the workloads used to create the synthetic workload
        wl_jobs = []
        for wl_entry in self.config_classification['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_entry)
            wl_jobs.extend(wl.jobs)

            # Apply clustering
            print "applying clustering on {}".format(wl_entry)
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
            requested_submit_rate = real_submit_rate * self.config_generator['submit_rate_factor']
            n_modelled_jobs = int(requested_submit_rate * self.config_generator['total_submit_interval'])

            if not n_modelled_jobs:
                print_colour("orange", "Low submit rate (real={} jobs/sec, requested={} jobs/sec), => number of jobs={}"
                             .format(real_submit_rate, requested_submit_rate, n_modelled_jobs))

                # set n jobs to one! (the very minimum)
                n_modelled_jobs = 1

            start_times_vec = np.random.rand(n_modelled_jobs) * self.config_generator['total_submit_interval']

            print "real_submit_rate", real_submit_rate
            print "n_jobs", n_modelled_jobs

            # create model jobs from clusters and time rates..
            modelled_jobs = model_jobs_from_clusters(cluster['cluster_matrix'],
                                                     start_times_vec,
                                                     nprocs=self.config_generator['sa_n_proc'],
                                                     nnodes=self.config_generator['sa_n_nodes']
                                                     )

            # create synthetic workload from modelled jobs
            # TODO: append workload tag to the synthetic app here!
            modelled_sa_jobs = []
            for cc, job in enumerate(modelled_jobs):
                app = SyntheticApp(
                    job_name="RS-appID-{}".format(cc),
                    time_signals=job.timesignals,
                    ncpus=self.config_generator['sa_n_proc'],
                    nnodes=self.config_generator['sa_n_nodes'],
                    time_start=job.time_start
                )

                modelled_sa_jobs.append(app)

            self.modelled_sa_jobs.extend(modelled_sa_jobs)
