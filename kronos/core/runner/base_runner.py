# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from kronos.core.config.config import Config


class BaseRunner(object):
    """
    Runner base class:
        -- defines a run mode
    """

    def __init__(self, config):

        assert isinstance(config, Config)

        self.config = config
        self.check_config()

    def check_config(self):
        """
        checks and sets default config options
        """
        pass

    def run(self):
        """
        Run the model according to the configuration options
        """
        raise NotImplementedError("Must be implemented in derived class..")

    def plot_results(self):
        """
        Plot desired results
        """
        raise NotImplementedError("Must be implemented in derived class..")
