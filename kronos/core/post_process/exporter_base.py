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

    class_export_type = "BaseExporter"
    default_export_format = None
    optional_configs = []

    def __init__(self, sim_set=None):

        # simulations data
        self.sim_set = sim_set
        self.export_config = None

    def export(self, export_config, output_path, job_classes, **kwargs):

        self.export_config = export_config

        self.check_export_config(export_config, output_path, job_classes, **kwargs)

        # Get output format from config (to choose appropriate method from the exporter..)
        export_format = export_config.get("format", self.default_export_format)

        # call the export function as appropriate
        if export_format:
            self.export_function_map(export_format)(output_path, job_classes, **kwargs)
        else:
            self.export_function_map(self.default_export_format)(output_path, job_classes, **kwargs)

    def check_export_config(self, export_config, out_path, job_classes, **kwargs):

        # create output dir if it does not exists..
        if not os.path.isdir(out_path):
            os.mkdir(out_path)

        # check that export type is consistent with the class type
        if export_config["type"] != self.class_export_type:
            raise ConfigurationError("Export type {}, does not match class: {}".format(export_config["type"],
                                                                                       self.__class__.__name__))
        # check that export format is consistent with export type
        if not self.export_function_map(export_config["format"]):
            raise ConfigurationError("Format type {}, not implemented for class: {}".format(export_config["format"],
                                                                                            self.__class__.__name__))
        if not self.optional_configs and kwargs:
            raise ConfigurationError("Class: {} does not accept optional config keys!".format(self.__class__.__name__))
        else:
            if not all(k in self.optional_configs for k in kwargs.keys()):
                for k in kwargs.keys():
                    if k not in self.optional_configs:
                        print "Class: {} incompatible with config {}".format(self.__class__.__name__, k)
                raise ConfigurationError

    @classmethod
    def export_function_map(cls, keys):
        raise NotImplementedError
