# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import logging

import numpy as np
from kronos_modeller.config.config import Config
from kronos_modeller.report import ModelMeasure, Report

from kronos_modeller.synthetic_app import SyntheticWorkload, SyntheticApp
from kronos_modeller.workload_filling import job_filling_types
from kronos_modeller.kronos_exceptions import ConfigurationError

from kronos_modeller.workload_data import WorkloadData
from kronos_modeller.workload_editing import workload_editing_types
from kronos_modeller.workload_modelling import workload_modelling_types


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

        assert all(isinstance(wl, WorkloadData) for wl in workloads)
        assert isinstance(config, Config)

        # configuration parameters
        self.config = config
        self.config_workload_filling = None
        self.config_workload_editing = None
        self.config_workload_modelling = None
        self.config_schedule_exporting = None

        self.workloads = workloads
        self.total_metrics_wl_orig = {}

        self.jobs_for_clustering = []
        self.clusters = []
        self.modelled_sa_jobs = []

        self.synthetic_apps = None
        self.sa_workload = None
        self.tot_n_jobs_wl_original = None
        self.real_wl_duration = None

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
        1) Applies user instructions on potentially partially filled workloads
        2) Generate a workload model through clustering+generation process
        3) Export the workload as a synthetic workload
        :return:
        """

        # 1) Apply fill-in strategies
        if self.config_workload_filling:
            self._apply_workload_filling()

        # 2) Edit the workloads (if necessary)
        if self.config_workload_editing:
            self._apply_workload_editing()

        # 3) Applies modelling techniques to jobs
        if self.config_workload_modelling:
            self._apply_workload_modelling()

        # 4) Generated synthetic apps from workloads
        if self.config_schedule_exporting:
            self._apply_schedule_exporting()

    def _apply_workload_filling(self):
        """
        Fill in possibly missing information on the workloads
        :return:
        """

        # call functions corresponding to fill_in types
        for filling_strategy_config in self.config_workload_filling['operations']:

            # select the appropriate workload_filling strategy
            filler = job_filling_types[filling_strategy_config["type"]](self.workloads)

            # configure the strategy with specific config + user defined functions
            filling_strategy_config.update({"user_functions": self.config_workload_filling['user_functions']})

            filler.apply(filling_strategy_config)

    def _apply_workload_editing(self):
        """
        Edit workloads (e.g. split workloads, etc..)
        :return:
        """

        # call functions corresponding to fill_in types
        for wl_edit_config in self.config_workload_editing:

            # select the appropriate workload_filling strategy
            editor = workload_editing_types[wl_edit_config["type"]](self.workloads)
            editor.apply(wl_edit_config)

    def _apply_workload_modelling(self):
        """
        Apply workload modelling strategies
        (not strictly required - if no modelling is applied
        the synthetic apps will be generated from un-modelled
        workloads
        :return:
        """

        # select the appropriate workload_filling strategy
        workload_modeller = workload_modelling_types[self.config_workload_modelling["type"]](self.workloads)
        workload_modeller.apply(self.config_workload_modelling)

        # explicitly return the model jobs
        model_jobs = workload_modeller.get_model_jobs()

        # TODO: check this step (multiple workloads might need to be retained..)
        self.workloads = [WorkloadData(model_jobs)]

    def _apply_schedule_exporting(self):
        """
        Export the synthetic workload generated from the model (or from the one-to-one translation
        of the KProfile into the KSchedule)
        :return:
        """

        # Simply convert the model workloads into synthetic apps
        self.synthetic_apps = []
        synapp_counter = 0
        for ww, wl in enumerate(self.workloads):
            for cc, job in enumerate(wl.jobs):
                app = SyntheticApp(
                    job_name="job-{}".format(synapp_counter),
                    time_signals=job.timesignals,
                    ncpus=job.ncpus,
                    time_start=job.time_start,
                    label="WL{}-JOB{}-ID{}".format(ww, cc, synapp_counter)
                )

                self.synthetic_apps.append(app)

                synapp_counter += 1

        # set up the synthetic workload
        sa_workload = SyntheticWorkload(self.config, self.synthetic_apps)

        # calculate the discretisation error (model to syn-apps)
        self.calculate_model_to_synapp_error(sa_workload)

        # export the synthetic workload
        kschedule_path = os.path.join(self.config.dir_output, self.config.kschedule_filename)
        sa_workload.export_kschedule(kschedule_path)

    def calculate_model_to_synapp_error(self, sa_workload):
        """
        Calculation of error produced by discretising the model jobs into
        synthetic applications. The error gets appended to the unique modelling "Report"
        :return:
        """

        # calculate the total values measure
        tot_apps = sa_workload.total_metrics_dict()

        relative_metrics_totals = {k: np.abs(v - tot_apps[k]) / float(v) * 100.0
                                   for k, v in self.total_metrics_wl_orig.iteritems()}

        msr = ModelMeasure("Scaled metrics sums - error [%]", relative_metrics_totals, __name__)
        Report.add_measure(msr)

        # calculate the measure relative to the number of jobs..
        # Calc max execution time for all the workloads..
        t_min = min(j.time_start for wl in self.workloads for j in wl.jobs)
        t_max = max(j.time_start for wl in self.workloads for j in wl.jobs)
        self.real_wl_duration = t_max - t_min

        if self.real_wl_duration:
            submit_interval = self.config_workload_modelling["job_submission_strategy"]['total_submit_interval']
            t_scaling = submit_interval / self.real_wl_duration
        else:
            t_scaling = 0.0

        dt_orig = self.real_wl_duration
        dt_sapps = sa_workload.max_sa_time_interval()

        rel_time_interval = (dt_orig - dt_sapps / t_scaling) / dt_orig * 100.
        msr = ModelMeasure("relative_time_interval [%]", rel_time_interval, __name__)
        Report.add_measure(msr)
