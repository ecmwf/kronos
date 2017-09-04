# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections import OrderedDict


class SimulationSet(object):
    """
    A set of simulation data
    """

    def __init__(self, sims):

        # List of simulations
        self.sims = sims

        # Rates (per class)
        self.rates = {}

        # calculate the stats and aggregate them by job classes
        self.class_stats_sums = self._calculate_class_stats_sums()

    def ordered_sims(self):
        return OrderedDict([(sim.name, sim) for sim in self.sims])

    def _calculate_class_stats_sums(self):
        """
        Calculate per class data of all the simulations in the set
        :return:
        """

        class_stats_sums = {}
        for sim in self.sims:
            class_stats_sums[sim.name] = sim.class_stats_sums()

        return class_stats_sums
