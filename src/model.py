import os
import numpy as np

import data_analysis
import workload_data

from jobs import model_jobs_from_clusters
from synthetic_app import SyntheticApp, SyntheticWorkload
from kronos_tools.print_colour import print_colour
from config.config import Config


class KronosModel(object):
    """
    The model class operates on a list of workloads:
    1) applies corrections to the worklaods according to the configuration instructions passed by the user
    2) applies a modelling strategy
    3) returns a synthetic workload
    """

    def __init__(self, workloads, config):

        assert all(isinstance(wl, workload_data.WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        self.config = config
        self.workloads = workloads
        self.jobs_for_clustering = []
        self.modelled_sa_jobs = []

    def generate_model(self):
        """
        takes the list of workloads and applies user instructions to generate a model
        :return:
        """

        # 1) apply user strategies
        self._apply_workload_straegies()

        # 2) apply modelling pipeline
        self._apply_model_pipeline()

    def _apply_workload_straegies(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """
        defaults_values = self.config.model.get('defaults', None)
        look_up_table = self.config.model.get('lookup_table', None)
        recomm_system = self.config.model.get('recommender_system', None)

        # try default values first
        if defaults_values:
            self._apply_defaults(defaults_values)

        # apply a recommender system solution
        if recomm_system:
            self._apply_recommender_system(recomm_system)

        # try a look up table
        if look_up_table:
            self._apply_lookup_table(look_up_table)

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model
        :return:
        """
        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)
        sa_workload.set_tuning_factors(self.config.model['model_pipeline']['tuning_factors'])

        # export the synthetic workload
        ksf_path = os.path.join(self.config.dir_output, self.config.ksf_filename)
        sa_workload.export_ksf(ksf_path, self.config.model['model_pipeline']['sa_n_frames'])

    def postprocess(self):
        """
        Postprocess the results of the run
        :return:
        """
        pass

    def _apply_defaults(self, defaults_values):
        """
        Apply default values if specified
        :param defaults_values:
        :return:
        """
        for def_k, def_v in defaults_values.items():
            for wl in self.workloads:
                if wl.tag in def_v['apply_to']:
                    wl.apply_default_metrics(def_v['metrics'])

    def _apply_lookup_table(self, look_up_table):
        """
        Apply a lookup table to add metrics from jobs in a workload to another
        :param look_up_table:
        :return:
        """

        assert isinstance(look_up_table, dict)

        # apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0
        for wl_source_tag in look_up_table['source_workloads']:
            wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
            for wl_dest_tag in look_up_table['apply_to']:
                wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                n_destination_jobs += len(wl_dest.jobs)
                n_job_matched += wl_dest.apply_lookup_table(wl_source, look_up_table['similarity_threshold'])

        print_colour("white", "jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))

    def _apply_recommender_system(self, recomm_system):
        """
        This implements the recommender system corrections
        :param recomm_system:
        :return:
        """
        print "TO IMPLEMENT RECOMMENDER SYSTEM"
        return recomm_system

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
        cluster_handler = data_analysis.factory(self.config.model['model_pipeline']['clustering']['name'],
                                                self.config.model['model_pipeline']['clustering'])

        cluster_handler.cluster_jobs(jobs)
        clusters_matrix = cluster_handler.clusters
        clusters_labels = cluster_handler.labels

        return clusters_matrix, clusters_labels

    def _apply_model_pipeline(self):
        """
        Apply modelling pipeline to selected workloads (listed in model_pipeline dictionary)
        :param model_pipeline:
        :return:
        """

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        self._check_jobs()

        model_pipeline = self.config.model.get('model_pipeline', None)

        total_submit_interval = model_pipeline['total_submit_interval']
        submit_rate_factor = model_pipeline['submit_rate_factor']

        # loop over all the workloads used to create the synthetic workload
        for wl_entry in model_pipeline['apply_to']:

            # retrieve all the jobs of this entry
            wl_jobs = []
            for wl_tag in wl_entry:
                wl = next(wl for wl in self.workloads if wl.tag == wl_tag)
                wl_jobs.extend(wl.jobs)

            # Apply clustering
            clusters_matrix, clusters_labels = self._apply_clustering(wl_jobs)

            # calculate the submittal rate from the selected workload
            submit_times = [j.time_queued for j in wl_jobs]
            real_submit_rate = float(len(submit_times)) / (max(submit_times) - min(submit_times))
            requested_submit_rate = real_submit_rate * submit_rate_factor
            n_modelled_jobs = int(requested_submit_rate * total_submit_interval)

            if not n_modelled_jobs:
                print_colour("orange", "Low submit rate (real={} jobs/sec, requested={} jobs/sec), => number of jobs={}"
                             .format(real_submit_rate, requested_submit_rate, n_modelled_jobs))

                # set n jobs to one! (the very minimum)
                n_modelled_jobs = 1

            start_times_vec = np.random.rand(n_modelled_jobs) * total_submit_interval

            print "real_submit_rate", real_submit_rate
            print "n_jobs", n_modelled_jobs

            # create model jobs from clusters and time rates..
            modelled_jobs = model_jobs_from_clusters(clusters_matrix,
                                                     start_times_vec,
                                                     nprocs=model_pipeline['sa_n_proc'],
                                                     nnodes=model_pipeline['sa_n_nodes']
                                                     )

            # create synthetic workload from modelled jobs
            # TODO: append workload tag to the synthetic app here!
            modelled_sa_jobs = []
            for cc, job in enumerate(modelled_jobs):
                app = SyntheticApp(
                    job_name="RS-appID-{}".format(cc),
                    time_signals=job.timesignals,
                    ncpus=model_pipeline['sa_n_proc'],
                    nnodes=model_pipeline['sa_n_nodes'],
                    time_start=job.time_start
                )

                modelled_sa_jobs.append(app)

            self.modelled_sa_jobs.extend(modelled_sa_jobs)
        # --------------------------------------------------------------------------


