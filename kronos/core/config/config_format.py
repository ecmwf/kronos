# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import os
from datetime import datetime

import strict_rfc3339

from kronos.io.json_io_format import JSONIoFormat


class ConfigFormat(JSONIoFormat):
    """
    A standardised format for profiling information.
    """
    format_version = 1
    format_magic = "KRONOS-CONFIG-MAGIC"
    schema_json = os.path.join(os.path.dirname(__file__), "config_schema.json")

    def __init__(self, created=None, uid=None):

        super(ConfigFormat, self).__init__(created=created, uid=uid)

    @classmethod
    def from_json_data(cls, data):
        """
        Given loaded and validated JSON data, actually do something with it
        """
        return cls(
            created=datetime.fromtimestamp(strict_rfc3339.rfc3339_to_timestamp(data['created'])),
            uid=data['uid']
        )

    def output_dict(self):
        """
        Obtain the data to be written into the file. Extends the base class implementation
        (which includes headers, etc.)
        """
        output_dict = super(ConfigFormat, self).output_dict()

        return output_dict
