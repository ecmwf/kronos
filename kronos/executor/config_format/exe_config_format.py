# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
from kronos.core.config_format_base import ConfigFormatBase


class ExeConfigFormat(ConfigFormatBase):
    """
    This class represents the format used for kronos export configuration file
    """
    schema_json = os.path.join(os.path.dirname(__file__), "exe_config_schema.json")
