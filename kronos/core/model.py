# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os

import numpy as np

import data_analysis
import workload_data
from config.config import Config
from exceptions_iows import ConfigurationError
from kronos.core.job_generation import generator
from kronos.core.kronos_tools.gyration_radius import r_gyration
from kronos.core.report import Report, ModelMeasure
from kronos_tools.print_colour import print_colour
from synthetic_app import SyntheticWorkload
from workload_fill_in import WorkloadFiller


class KronosModel(object):
    """
    The model class operates on a list of workloads:
    1) applies corrections to the workloads according to the configuration instructions passed by the user
    2) applies a modelling strategy
    3) returns a synthetic workload
    """

    required_config_fields = [
        # 'classification',
        # 'generator',
    ]

    def __init__(self, workloads, config):

        assert all(isinstance(wl, workload_data.WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        # configuration parameters
        self.config = config
        self.config_fill_in = None
        self.config_classification = None
        self.config_generator = None

        self.workloads = workloads
        self.total_metrics_wl_orig = {}

        self.jobs_for_clustering = []
        self.clusters = []
        self.modelled_sa_jobs = []

        self.sa_workload = None
        self.tot_n_jobs_wl_original = None
        self.tot_duration_wl_original = None

        # check that there is the "model" entry in the config file..
        if not self.config.model:
            raise ConfigurationError("'model' entry not set in config file, but required!")

        # check that all the required fields are set
        for req_item in self.required_config_fields:
            if req_item not in self.config.model.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))

        # check that configuration keys are correct
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
        if self.config_fill_in:
            self._apply_workload_fillin()

            # calculate metrics of total sums over all the original workloads
            # TODO: the calculation of the original sums should go somewhere else..
            self.total_metrics_wl_orig = {}
            for wl in self.workloads:
                tot = wl.total_metrics_sum_dict
                for k, v in tot.iteritems():

                    if self.total_metrics_wl_orig.get(k):
                        self.total_metrics_wl_orig[k] += v
                    else:
                        self.total_metrics_wl_orig[k] = v

            self.tot_n_jobs_wl_original = len([j for wl in self.workloads for j in wl.jobs])

            # Calc max execution time for all the workloads..
            t_min = min(j.time_start for wl in self.workloads for j in wl.jobs)
            t_max = max(j.time_start for wl in self.workloads for j in wl.jobs)
            self.tot_duration_wl_original = t_max - t_min

        # 2) apply classification
        if self.config_classification:
            self._apply_classification()

        # 3) apply generation
        if self.config_generator:
            self._apply_generation()
            self.generate_synthetic_workload()

    def _apply_workload_fillin(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """

        filler = WorkloadFiller(self.config_fill_in, self.workloads)

        # call functions corresponding to fill_in types
        for operation in self.config_fill_in['operations']:
            getattr(filler, operation['type'])()

    def generate_synthetic_workload(self):

        if not self.modelled_sa_jobs:
            raise ConfigurationError("cannot export before generating the model!")

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)
        sa_workload.scaling_factors = self.config_generator['scaling_factors']
        self.sa_workload = sa_workload

        # calculate the total values measure
        tot_apps = self.sa_workload.total_metrics_dict()
        relative_metrics_totals = {k: np.abs(v - tot_apps[k]) / float(v) * 100.0
                                   for k, v in self.total_metrics_wl_orig.iteritems()}
        Report.add_measure(ModelMeasure("relative_totals [%]", relative_metrics_totals, __name__))

        # calculate the measure relative to the number of jobs..
        t_scaling = self.config_generator['total_submit_interval'] / self.tot_duration_wl_original
        dt_orig = self.tot_duration_wl_original
        dt_sapps = sa_workload.max_sa_time_interval()
        Report.add_measure(ModelMeasure("relative_time_interval [%]", (dt_orig-dt_sapps/t_scaling)/dt_orig*100., __name__))

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model
        :return:
        """

        ksf_path = os.path.join(self.config.dir_output, self.config.ksf_filename)
        self.sa_workload.export_ksf(ksf_path)

    def _check_jobs(self):
        """
        Check that all jobs have the minimum required fields..
        :return:
        """
        print_colour("green", "Checking all jobs before classification..")

        # check only the jobs to be used for classification..
        for wl_name in self.config_classification['clustering']['apply_to']:

            try:
                wl = next(wl for wl in self.workloads if wl.tag == wl_name)
                wl.check_jobs()
            except StopIteration:
                raise ValueError(" workload named {} not found!".format(wl_name))

    def _apply_classification(self):
        """
        Apply modelling classification to selected workloads
        :return:
        """

        # apply operations on workloads (if required..)
        class_operations_config = self.config_classification.get('operations', None)

        if class_operations_config is not None:
            cutout_workloads = []
            for op_config in class_operations_config:

                if op_config['type'] == "split":

                    # check splitting function configuration
                    required_fields = [
                        'apply_to',
                        'create_workload',
                        'split_by',
                        'keywords_in',
                        'keywords_out'
                    ]

                    # check that all the required fields are set
                    for req_item in required_fields:
                        if req_item not in op_config.keys():
                            raise ConfigurationError("'step_function' requires to specify {}".format(req_item))

                    print_colour("green", "Splitting workload {}".format(op_config['apply_to']))
                    wl = next(wl for wl in self.workloads if wl.tag == op_config['apply_to'])
                    sub_workloads = wl.split_by_keywords(op_config)
                    print_colour("cyan", "splitting has created workload {} with {} jobs".format(sub_workloads.tag, len(sub_workloads.jobs)))

                    # accumulate cutout worklaods
                    cutout_workloads.append(sub_workloads)

            self.workloads.extend(cutout_workloads)

        # save the KPF's of the sub-workloads before attempting the clustering
        save_wl_before_clustering = self.config_classification.get('save_wl_before_clustering', False)
        if save_wl_before_clustering:
            # for wl in self.workloads:
            #     kpf_hdl = ProfileFormat(model_jobs=wl.jobs, workload_tag=wl.tag)
            #     kpf_hdl.write_filename(os.path.join(self.config.dir_output, wl.tag+"_workload.kpf"))
            wl_group = workload_data.WorkloadDataGroup(cutout_workloads)
            wl_group.export_pickle(os.path.join(self.config.dir_output, "_workload"))

        # check validity of jobs before doing the actual modelling..
        # NB: the preliminary phase of workload manipulation (defaults, lookup tables and recommender sys
        # should have produced a set of "complete" and therefore valid jobs)
        self._check_jobs()

        # loop over all the workloads used to create the synthetic workload
        clustering_config = self.config_classification['clustering']
        for wl_entry in clustering_config['apply_to']:
            wl = next(wl for wl in self.workloads if wl.tag == wl_entry)

            print_colour("green", "-------> applying classification to workload {}".format(wl_entry))

            # Apply clustering
            cluster_handler = data_analysis.factory(clustering_config['type'], clustering_config)
            job_signal_matrix = wl.jobs_to_matrix(clustering_config['num_timesignal_bins'])
            cluster_handler.cluster_jobs(job_signal_matrix)
            clusters_matrix = cluster_handler.clusters
            clusters_labels = cluster_handler.labels

            # check values in the matrix
            for row in clusters_matrix:
                if (row < 0.).any():
                    print "value < 0 encountered after clustering! corrected to 0."
                    row[row < 0.] = 0.

            # calculate the mean radius of gyration (among all clusters) for each sub-workload
            r_sub_wl_mean = []
            matrix_jobs_in_cluster = []
            nbins = clustering_config['num_timesignal_bins']
            for cc in range(clusters_matrix.shape[0]):
                matrix_jobs_in_cluster = np.asarray([j.ts_to_vector(nbins) for j in np.asarray(wl.jobs)[clusters_labels == cc]])
                r_sub_wl_mean.append(r_gyration(matrix_jobs_in_cluster))

            self.clusters.append({
                                  'source-workload': wl_entry,
                                  'jobs_for_clustering': wl.jobs,
                                  'cluster_matrix': clusters_matrix,
                                  'labels': clusters_labels,
                                  'r_gyration': r_sub_wl_mean,
                                  'matrix_jobs_in_cluster': matrix_jobs_in_cluster
                                  })

    def _apply_generation(self):
        """
        Generate synthetic workload form the supplied model jobs
        :return:
        """

        global_t0 = min(j.time_start for cl in self.clusters for j in cl['jobs_for_clustering'])
        global_tend = max(j.time_start for cl in self.clusters for j in cl['jobs_for_clustering'])

        # check generation configuration keys
        required_fields = [
            "type",
            "n_bins_for_pdf",
            "random_seed",
            "scaling_factors",
            "submit_rate_factor",
            "synthapp_n_cpu",
            "total_submit_interval"
        ]

        # check that all the required fields are set
        for req_item in required_fields:
            if req_item not in self.config_generator.keys():
                raise ConfigurationError("'generator' requires to specify {}".format(req_item))

        sapps_generator = generator.SyntheticWorkloadGenerator(self.config_generator,
                                                               self.clusters,
                                                               global_t0,
                                                               global_tend,
                                                               n_bins_for_pdf=self.config_generator['n_bins_for_pdf'],
                                                               n_bins_timesignals=self.config_classification['clustering']['num_timesignal_bins'])

        self.modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

        # Calculate the radius of gyration of clusters and generated model jobs
        r_gyr_all_pc = {}
        r_gyr_modeljobs_all = sapps_generator.calculate_modeljobs_r_gyrations()
        for cluster in self.clusters:
            r_gyr_wl = np.mean(cluster["r_gyration"])
            r_gyr_modeljobs = np.mean(r_gyr_modeljobs_all[cluster["source-workload"]])
            r_gyr_all_pc[cluster["source-workload"]] = np.abs(r_gyr_modeljobs-r_gyr_wl)/r_gyr_wl*100

        Report.add_measure(ModelMeasure("relative_r_gyration [%]", r_gyr_all_pc, __name__))

