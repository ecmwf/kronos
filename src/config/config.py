import os
import json

from exceptions_iows import ConfigurationError


class Config(object):
    """
    A storage place for global configuration, including parsing of input JSON, and error checking
    """

    # name of the plugin to load up
    plugin = None
    runner = None
    controls = None
    post_process = None
    ksf_filename = 'schedule.ksf'
    # ---------------------------------------------------------

    # Directory choices
    dir_output = None
    dir_input = None
    kpf_file = None
    defaults = None
    model = None

    ingestion = {}

    # Modelling and data_analysis details
    model_clustering = "none"
    model_clustering_algorithm = None
    model_scaling_factor = 1.0

    # hardware
    CPU_FREQUENCY = 2.3e9  # Hz

    # Debugging info
    verbose = False

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

        for k, v in config_dict.iteritems():
            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))
            setattr(self, k, v)

        # And any necessary actions
        if not self.dir_input:
            raise ConfigurationError("input folder not set")

        if not self.dir_output:
            raise ConfigurationError("output folder not set")

        # if input or output folders do not exist, an error is raised
        if not os.path.exists(self.dir_input):
            raise ConfigurationError("input folder {} - does not exist!".format(self.dir_input))

        if not os.path.exists(self.dir_output):
            raise ConfigurationError("output folder {} - does not exist!".format(self.dir_output))
