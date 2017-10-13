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

        # # calculate the stats and aggregate them by job classes
        # self.class_stats_sums = self._calculate_class_stats_sums(class_list)
        self.class_stats_sums = None

    def ordered_sims(self):
        return OrderedDict([(sim.name, sim) for sim in self.sims])

    def calculate_class_stats_sums(self, class_list):
        """
        Calculate per class data of all the simulations in the set
        :return:
        """

        class_stats_sums = {}
        for sim in self.sims:
            class_stats_sums[sim.name] = sim.class_stats_sums(class_list)

        self.class_stats_sums = class_stats_sums

    def retrieve_common_job_classes(self, class_list):
        """
        Retrieve the class names of the classes that are common among the simulations in the set
        :param class_list:
        :return:
        """

        sim_classes_all = []
        for class_tp in class_list:

            sim_classes = []
            for sim in self.sims:
                for job in sim.jobs:
                    if job.is_in_class(class_tp):
                        sim_classes.append(tuple(class_tp))

                sim_classes_all.append(set(sim_classes))

        common_classes = set.intersection(*sim_classes_all)

        return common_classes
