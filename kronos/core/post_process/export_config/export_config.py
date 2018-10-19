# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os
import json
import datetime
from collections import OrderedDict

from kronos.core.exceptions_iows import ConfigurationError
from kronos.core.post_process.export_config.export_config_format import ExportConfigFormat


class ExportConfig(object):

    output_path = None
    job_classes = None
    simulation_labels = None
    simulation_paths = None
    exports = None
    n_procs_node = None

    def __init__(self, conf_dict):

        # configuration dictionary
        self._config_dict = conf_dict

        # check that the config are consistent with the schema
        self.ckeck_config()

    def ckeck_config(self):
        """
        Check the configuration
        :return:
        """

        # check the json data against the post-processing config schema
        ExportConfigFormat().validate_json(self._config_dict)

        # check all the configurations
        for k, v in self._config_dict.iteritems():

            if not hasattr(self, k):
                raise ConfigurationError("Unexpected configuration keyword provided - {}:{}".format(k, v))

            # if OK, set this attribute..
            setattr(self, k, v)

        # take the timestamp to be used to archive run folders (if existing)
        out_dir = self._config_dict["output_path"]
        if os.path.exists(out_dir):
            time_stamp_now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            time_stamped_outdir = out_dir + "." + time_stamp_now
            print "Dir: {} already exists!\n..moving it to: {}".format(out_dir, time_stamped_outdir)
            os.rename(out_dir, time_stamped_outdir)

    @classmethod
    def from_json_file(cls, filename):

        # Read the config file
        with open(filename, "r") as f:
            config_options = json.load(f, object_pairs_hook=OrderedDict)

        return cls(config_options)

