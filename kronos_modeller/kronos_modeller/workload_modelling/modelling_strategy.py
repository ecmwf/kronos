# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging

from kronos_modeller.strategy_base import StrategyBase

logger = logging.getLogger(__name__)


class WorkloadModellingStrategy(StrategyBase):
    """
    Class that apply some modelling into the workloads
    """

    required_config_fields = [
        "type",
        "scaling_factors",
        "global_scaling_factor"
    ]

    def __init__(self, workloads):

        super(WorkloadModellingStrategy, self).__init__(workloads)

        # a modelling strategy will generate a modelled workload (set)
        self.model_jobs = []
        self.workload_set = None

    def get_model_jobs(self):
        """
        Explicitly return the model jobs
        :return:
        """

        if not self.model_jobs:
            logger.error("Modelled jobs seem empty - something went wrong..")

        return self.model_jobs

    def get_workload_set(self):
        """
        Explicitly return a set of workloads
        :return:
        """

        if not self.model_jobs:
            logger.error("Modelled jobs seem empty - something went wrong..")

        if not self.workload_set:
            logger.error("Modelled workloads seem empty - something went wrong..")

        return self.workload_set
