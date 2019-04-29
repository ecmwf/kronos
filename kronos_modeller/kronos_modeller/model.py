# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import logging
import os

import numpy as np
from kronos_modeller.job_clustering import clustering_types
from kronos_modeller.job_filling import job_filling_types
from kronos_modeller.workload_editing import workload_editing_types
from kronos_modeller.job_generation import generator
from kronos_modeller.kronos_tools.gyration_radius import r_gyration
from kronos_modeller.report import Report, ModelMeasure

import kronos_modeller.workload_data_group
import workload_data
from config.config import Config
from kronos_exceptions import ConfigurationError
from synthetic_app import SyntheticWorkload, SyntheticApp

logger = logging.getLogger(__name__)


class KronosModel(object):
    """
    The model class operates on a list of workloads:
    1) applies corrections to the workloads according to the configuration instructions passed by the user
    2) applies a modelling strategy
    3) returns a synthetic workload
    """

    required_config_fields = []

    def __init__(self, workloads, config):

        assert all(isinstance(wl, workload_data.WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        # configuration parameters
        self.config = config
        self.config_job_filling = None
        self.config_job_clustering = None
        self.config_schedule_generation = None
        self.config_workload_editing = None

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
        if self.config_job_filling:
            self._apply_workload_fillin()

        if self.config_workload_editing:
            self._apply_workload_editing()

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
        if self.config_job_clustering:
            self._apply_classification()

        # 3) apply generation
        if self.config_job_clustering and self.config_schedule_generation:
            self._apply_generation()
            self.generate_synthetic_workload()

        # in the case neither the classification nor the generation entries are specified in the config file,
        # just translate the KProfile file into the KSchedule file in a one-to-one relationship
        if not self.config_job_clustering:
            self._kschedule_from_kprofile_one_to_one()

    def _apply_workload_fillin(self):
        """
        Apply user requested strategies to the workloads
        :return:
        """

        # call functions corresponding to fill_in types
        for filling_strategy_config in self.config_job_filling['operations']:

            # select the appropriate job_filling strategy
            filler = job_filling_types[filling_strategy_config["type"]](self.workloads)

            # configure the strategy with specific config + user defined functions
            filling_strategy_config.update({"user_functions": self.config_job_filling['user_functions']})

            filler.apply(filling_strategy_config)

    def _apply_workload_editing(self):

        # call functions corresponding to fill_in types
        for wl_edit_config in self.config_workload_editing:

            # select the appropriate job_filling strategy
            editor = workload_editing_types[wl_edit_config["type"]](self.workloads)
            editor.apply(wl_edit_config)

    def _apply_classification(self):
        """
        Apply job classification to selected workloads
        and sets the clusters later used for schedule generation
        :return:
        """

        # select the appropriate job_filling strategy
        classifier = clustering_types[self.config_job_clustering["type"]](self.workloads)
        classifier.apply(self.config_job_clustering)
        self.clusters = classifier.get_clusters()

    def generate_synthetic_workload(self):

        if not self.modelled_sa_jobs:
            raise ConfigurationError("cannot export before generating the model!")

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)

        # set up the scaling factors as individual sc * global sc
        scaling_facts = self.config_schedule_generation['scaling_factors']
        scaling_global = self.config_schedule_generation['global_scaling_factor']
        sa_workload.scaling_factors = {k: v*scaling_global for k, v in scaling_facts.iteritems()}

        self.sa_workload = sa_workload

        # calculate the total values measure
        tot_apps = self.sa_workload.total_metrics_dict()
        relative_metrics_totals = {k: np.abs(v - tot_apps[k]) / float(v) * 100.0
                                   for k, v in self.total_metrics_wl_orig.iteritems()}
        Report.add_measure(ModelMeasure("Scaled metrics sums - error [%]", relative_metrics_totals, __name__))

        # calculate the measure relative to the number of jobs..
        if self.tot_duration_wl_original:
            t_scaling = self.config_schedule_generation['total_submit_interval'] / self.tot_duration_wl_original
        else:
            t_scaling = 0.0

        dt_orig = self.tot_duration_wl_original
        dt_sapps = sa_workload.max_sa_time_interval()
        Report.add_measure(ModelMeasure("relative_time_interval [%]",
                                        (dt_orig-dt_sapps/t_scaling)/dt_orig*100.,
                                        __name__))

    def export_synthetic_workload(self):
        """
        Export the synthetic workload generated from the model (or from the one-to-one translation
        of the KProfile into the KSchedule)
        :return:
        """

        kschedule_path = os.path.join(self.config.dir_output, self.config.kschedule_filename)
        self.sa_workload.export_kschedule(kschedule_path)

    def _check_jobs(self, workloads_to_check=None):
        """
        Check that all jobs have the minimum required fields..
        :return:
        """
        logger.info("Checking all jobs..")

        # check only the workloads specified (if passed to the function) otherwise check all the jobs
        workloads_to_check = workloads_to_check if workloads_to_check else [wl.tag for wl in self.workloads]

        # check only the jobs to be used for classification..
        for wl_name in workloads_to_check:

            try:
                wl = next(wl for wl in self.workloads if wl.tag == wl_name)
                wl.check_jobs()
            except StopIteration:
                raise ValueError(" workload named {} not found!".format(wl_name))

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
            if req_item not in self.config_schedule_generation.keys():
                raise ConfigurationError("'generator' requires to specify {}".format(req_item))

        n_bins_for_pdf = self.config_job_clustering['num_timesignal_bins']
        n_bins_timesignals = self.config_job_clustering['num_timesignal_bins']

        sapps_generator = generator.SyntheticWorkloadGenerator(self.config_schedule_generation,
                                                               self.clusters,
                                                               global_t0,
                                                               global_tend,
                                                               n_bins_for_pdf=n_bins_for_pdf,
                                                               n_bins_timesignals=n_bins_timesignals)

        self.modelled_sa_jobs = sapps_generator.generate_synthetic_apps()

        # Calculate the radius of gyration of clusters and generated model jobs
        r_gyr_all_pc = {}
        r_gyr_modeljobs_all = sapps_generator.calculate_modeljobs_r_gyrations()

        for cluster in self.clusters:
            r_gyr_wl = np.mean(cluster["r_gyration"])
            r_gyr_modeljobs = np.mean(r_gyr_modeljobs_all[cluster["source-workload"]])
            r_gyr_all_pc[cluster["source-workload"]] = np.abs(r_gyr_modeljobs-r_gyr_wl)/r_gyr_wl*100

        Report.add_measure(ModelMeasure("r_gyration error [%]", r_gyr_all_pc, __name__))

    def _kschedule_from_kprofile_one_to_one(self):
        """
        This function very simply translates model_jobs into
        synthetic applications with a one-to-one relationship
        :return:
        """

        logger.info("generating the KSchedule from the KProfile in a one-to-one relationship")

        # check that all the model jobs contain all the metrics
        self._check_jobs()

        # initialize the modelled synthetic apps
        self.modelled_sa_jobs = []

        for wl in self.workloads:

            for cc, job in enumerate(wl.jobs):
                app = SyntheticApp(
                    job_name="appID-{}".format(cc),
                    time_signals=job.timesignals,
                    ncpus=job.ncpus,
                    time_start=0.0,
                    label=job.label
                )

                self.modelled_sa_jobs.append(app)

        self.sa_workload = SyntheticWorkload(self.config, self.modelled_sa_jobs)

        # set up the scaling factors as individual sc * global sc
        scaling_facts = self.config_schedule_generation['scaling_factors']
        scaling_global = self.config_schedule_generation['global_scaling_factor']
        self.sa_workload.scaling_factors = {k: v*scaling_global for k, v in scaling_facts.iteritems()}

