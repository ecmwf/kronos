# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import json

from kronos.io.json_io_format import JSONIoFormat


class ConfigFormatBase(JSONIoFormat):
    """
    This class represents the base format used for generic kronos configuration files
    """
    schema_json = ""

    def __init__(self, data=None):
        super(ConfigFormatBase, self).__init__()
        self.data = data

    @classmethod
    def schema(cls):
        """
        Obtain the json schema for the given format
        """
        with open(cls.schema_json, 'r') as fschema:
            return json.load(fschema)

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(data)

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = {"config_data": self.data}

        return output_dict
