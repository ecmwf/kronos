# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os

from kronos.core.exceptions_iows import ConfigurationError


class ExporterBase(object):

    export_type = "BaseExporter"
    default_export_format = None
    optional_configs = []

    def __init__(self, sim_set=None):
        self.sim_set = sim_set
        self.export_config = None

    def export(self, export_config, output_path, job_classes, **kwargs):

        self.export_config = export_config
        self.check_export_config(export_config, output_path, **kwargs)
        self.do_export(export_config, output_path, job_classes, **kwargs)

    def check_export_config(self, export_config, out_path, **kwargs):

        # create output dir if it does not exists..
        if not os.path.isdir(out_path):
            os.mkdir(out_path)

        # check that export type is consistent with the class type
        if export_config["type"] != self.export_type:
            raise ConfigurationError("Export type {}, does not match class: {}".format(export_config["type"],
                                                                                       self.__class__.__name__))

        if not self.optional_configs and kwargs:
            raise ConfigurationError("Class: {} does not accept optional config keys!".format(self.__class__.__name__))
        else:
            if not all(k in self.optional_configs for k in kwargs.keys()):
                for k in kwargs.keys():
                    if k not in self.optional_configs:
                        print "Class: {} incompatible with config {}".format(self.__class__.__name__, k)
                raise ConfigurationError

    def do_export(self, export_config, output_path, job_classes, **kwargs):
        raise NotImplementedError
