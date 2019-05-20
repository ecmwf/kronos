# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import logging

from kronos_modeller.config.config import Config

from kronos_modeller.synthetic_app import SyntheticWorkload
from kronos_modeller.workload_filling import job_filling_types
from kronos_modeller.kronos_exceptions import ConfigurationError

from kronos_modeller.workload import Workload
from kronos_modeller.workload_editing import workload_editing_types
from kronos_modeller.workload_modelling import workload_modelling_types


logger = logging.getLogger(__name__)


class KronosModel(object):
    """
    The model class operates on a list of workloads and
    performs the following actions in sequence:
      1) Applies user instructions on potentially partially filled workloads
      2) Edit the workloads (e.g. splitting them)
      3) Generate a workload model through clustering+generation process
      4) Export the workload as a synthetic workload
    """

    # no required params at model level
    required_config_fields = []

    def __init__(self, workload_set, config):

        assert all(isinstance(wl, Workload) for wl in workload_set.workloads)
        assert isinstance(config, Config)

        # check that there is the "model" entry in the config file..
        if not config.model:
            raise ConfigurationError("'model' entry not set in config file, but required!")

        self.config = config
        self.workload_set = workload_set

        # check that there is the "model" entry in the config file..
        if not self.config.model:
            raise ConfigurationError("'model' entry not set in config file, but required!")

        # check that all the required fields are set
        for req_item in self.required_config_fields:
            if req_item not in self.config.model.keys():
                raise ConfigurationError("{} requires to specify {}".format(self.__class__.__name__, req_item))

    def generate_model(self):
        """
        takes the list of workloads and performs three actions in sequence:
        1) Applies user instructions on potentially partially filled workloads
        2) Edit the workloads (e.g. splitting them)
        3) Generate a workload model through clustering+generation process
        ) Export the workload as a synthetic workload
        :return:
        """

        # 1) Apply fill-in strategies
        if self.config.model.get("workload_filling"):
            self._apply_workload_filling()

        # 2) Edit the workloads (if necessary)
        if self.config.model.get("workload_editing"):
            self._apply_workload_editing()

        # 3) Applies modelling techniques to jobs
        if self.config.model.get("workload_modelling"):
            self._apply_workload_modelling()

        # 4) Generated synthetic apps from workloads
        if self.config.model.get("schedule_exporting"):
            self._apply_schedule_exporting()

    def _apply_workload_filling(self):
        """
        Fill in possibly missing information on the workloads
        :return:
        """

        # call functions corresponding to fill_in types
        for filling_strategy_config in self.config.model["workload_filling"].get('operations', []):

            # select the appropriate workload_filling strategy
            filler = job_filling_types[filling_strategy_config["type"]](self.workload_set.workloads)

            # configure the strategy with specific config + user defined functions (if present)
            if self.config.model["workload_filling"].get('user_functions'):
                user_functions = self.config.model["workload_filling"]['user_functions']
                filling_strategy_config.update({"user_functions": user_functions})

            filler.apply(filling_strategy_config)

    def _apply_workload_editing(self):
        """
        Edit workloads (e.g. split workloads, etc..)
        :return:
        """

        # call functions corresponding to fill_in types
        for wl_edit_config in self.config.model["workload_editing"]:

            # select the appropriate workload_filling strategy
            editor = workload_editing_types[wl_edit_config["type"]](self.workload_set.workloads)
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
        workload_modeller = workload_modelling_types[self.config.model["workload_modelling"]["type"]] \
            (self.workload_set.workloads)

        workload_modeller.apply(self.config.model["workload_modelling"])

        # get the newly created set of (modelled) workloads
        self.workload_set = workload_modeller.get_workload_set()

    def _apply_schedule_exporting(self):
        """
        Export the synthetic workload generated from the model
        (or from the one-to-one translation of the KProfile
        into the KSchedule)
        :return:
        """

        # generate synthetic apps from the workload set
        synthetic_apps = self.workload_set.generate_synapps_from_workloads()

        # set up the synthetic workload (from synapps and config)
        sa_workload = SyntheticWorkload(self.config, synthetic_apps)

        # Apply individual scaling factors to the time-series of the synthetic apps
        sc_facts = self.config.model["schedule_exporting"].get('scaling_factors')
        if sc_facts:
            sa_workload.scaling_factors = sc_facts

        # Apply global scaling factor to the synthetic apps (including #CPU's, etc..)
        sc_facts_g = self.config.model["schedule_exporting"].get('global_scaling_factor', 1.0)
        if sc_facts_g:
            sa_workload.glob_scaling_factor = sc_facts_g

        # Then export each synthetic app of the workload
        kschedule_path = os.path.join(self.config.dir_output, self.config.kschedule_filename)
        sa_workload.export_kschedule(kschedule_path)
