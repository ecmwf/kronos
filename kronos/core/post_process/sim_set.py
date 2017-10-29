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

    def calculate_class_stats_sums(self, job_classes):
        """
        Calculate per class data of all the simulations in the set
        :return:
        """

        class_stats_sums = {}
        for sim in self.sims:
            class_stats_sums[sim.name] = sim.class_stats_sums(job_classes)

        self.class_stats_sums = class_stats_sums

    def retrieve_common_job_classes(self, class_dict):
        """
        Retrieve the names of the classes for which there is at least one job in each simulation of the set
        :param class_dict:
        :return:
        """

        class_common_dict = {}
        for class_name, class_regex in class_dict.iteritems():

            print "checking job class {}".format(class_name)
            found_in_all_sims = True
            for sim in self.sims:
                found_in_sim = False
                for job in sim.jobs:
                    if job.is_in_class(class_regex):
                        found_in_sim = True
                if not found_in_sim:
                    found_in_all_sims = False

            if found_in_all_sims:
                class_common_dict[class_name] = class_regex

        return class_common_dict
