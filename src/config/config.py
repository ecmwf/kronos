import os
import sys
import json

from exceptions_iows import ConfigurationError
from kronos_tools.print_colour import print_colour


class Config(object):
    """
    A storage place for global configuration, including parsing of input JSON, and error checking
    """

    # Debugging info
    verbose = False

    # Directory choices
    dir_input = None
    dir_output = None
    kpf_files = []
    ksf_filename = 'schedule.ksf'

    model = None
    run = None
    analysis = None
    # ---------------------------------------------------------

    ingestion = {}

    # Modelling and data_analysis details
    model_clustering = "none"
    model_clustering_algorithm = None
    model_scaling_factor = 1.0

    # hardware
    CPU_FREQUENCY = 2.3e9  # Hz

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
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

        # ------------ check for all required fields --------------
        if not self.dir_input:
            raise ConfigurationError("input folder not set")

        if not self.dir_output:
            raise ConfigurationError("output folder not set")

        if not self.model:
            print_colour("orange", "Nothing to do, 'model' configuration is empty")
            sys.exit(0)

        # if input or output folders do not exist, an error is raised
        if not os.path.exists(self.dir_input):
            raise ConfigurationError("input folder {} - does not exist!".format(self.dir_input))

        if not os.path.exists(self.dir_output):
            raise ConfigurationError("output folder {} - does not exist!".format(self.dir_output))

