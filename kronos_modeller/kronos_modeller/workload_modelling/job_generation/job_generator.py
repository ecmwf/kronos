# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


class JobGenerator(object):
    """
    This class generates
    """

    def __init__(self, schedule_strategy, wl_clusters, config):

        self.schedule_strategy = schedule_strategy
        self.wl_clusters = wl_clusters
        self.config = config

    def generate_jobs(self):

        raise NotImplementedError("not implemented in base class")
