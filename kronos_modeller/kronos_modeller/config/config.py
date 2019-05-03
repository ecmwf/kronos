# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import json
import logging
import os

from config_format import ConfigFormat
from kronos_modeller import log_msg_format
from kronos_modeller.kronos_exceptions import ConfigurationError


class Config(object):
    """
    A storage place for global configuration, including parsing of input JSON, and error checking
    """

    # Debugging info
    verbose = False

    kronos_log_file = "kronos-modeller.log"

    version = None
    uid = None
    created = None
    tag = None

    # Directory choices
    dir_input = None
    dir_output = None
    kprofile_files = []
    kschedule_filename = 'time_schedule.kschedule'

    model = None
    run = None
    analysis = None
    # ---------------------------------------------------------

    ingestion = {}

    # Modelling and data_analysis details
    model_clustering = "none"
    model_clustering_algorithm = None
    model_scaling_factor = 1.0

    def __init__(self, config_dict=None, config_path=None):

        assert config_dict is None or config_path is None

        # If specified, load a config from the given JSON file. Custom modification to the JSON spec
        # permits lines to be commented out using a '#' character for ease of testing
        if config_path is not None:
            with open(config_path, 'r') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    if '#' in line:
                        lines[i] = line[:line.index('#')]

                config_dict = json.loads(''.join(lines))

        config_dict = config_dict if config_dict is not None else {}

        # Update the default values using the supplied configuration dict
        if not isinstance(config_dict, dict):
            raise ConfigurationError("no keys found in configuration file")

        # check all the configurations
        for k, v in config_dict.iteritems():

            print "checking for attribute {}..".format(k)
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

        # ------------ check against schema --------------
        ConfigFormat().validate_json(config_dict)

        # ---------------------------------------------
        # TODO: #nodes does not pass through the model (set by kronos_executor config for now..)
        if self.model:
            if self.model.get('schedule_generation'):
                self.model['schedule_generation']['synthapp_n_nodes'] = 1
        # ---------------------------------------------

        # if input or output folders do not exist, an error is raised
        if not os.path.exists(self.dir_input):
            raise ConfigurationError("input folder {} - does not exist!".format(self.dir_input))

        if not os.path.exists(self.dir_output):
            raise ConfigurationError("output folder {} - does not exist!".format(self.dir_output))

        # ----------------- logging setup --------------------
        root_logger = logging.getLogger()
        fh = logging.FileHandler(self.kronos_log_file, mode='w')
        fh.setFormatter(logging.Formatter(log_msg_format))
        fh.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        root_logger.addHandler(fh)
        # -----------------------------------------------------

