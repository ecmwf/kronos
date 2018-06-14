# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os
import json


class EventSchema(object):

    # JSON schema of a kronos event
    schema_json = os.path.join(os.path.dirname(__file__), "schema.json")

    @classmethod
    def schema(cls):
        """
        Obtain the json schema for a kronos event
        """

        print "read schema!!!!!!"

        with open(cls.schema_json, 'r') as fschema:
            return json.load(fschema)