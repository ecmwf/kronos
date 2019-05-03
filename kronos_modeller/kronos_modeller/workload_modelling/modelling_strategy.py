# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

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
