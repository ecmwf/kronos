# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os
import json
import jsonschema


class EventBase(object):

    """
    A Kronos event that can be triggered by jobs
    """

    # JSON schema of a kronos event
    schema_json = os.path.join(os.path.dirname(__file__), "schema.json")

    def __init__(self, message_json, validate_event=False):

        # keeps the json internally
        self.__message_json = message_json

        # check all the configurations
        if validate_event:
            self.validate_json(message_json)

        # keep the top-level keys as attributes
        for k, v in message_json.items():
            setattr(self, k, v)

    def get_hashed(self):
        """
        Hashed version of this event
        :return:
        """
        raise NotImplementedError

    def __str__(self):
        return "KRONOS-EVENT: type: {}; job: {}".format(self.__message_json["type"],
                                                        self.__message_json["info"].get("job"))

    def __bytes__(self):
        return str(self).encode('utf-8')

    def __eq__(self, other_event):
        """
        Used to match: an event VS job-dependency event
        """

        raise NotImplementedError

    @classmethod
    def schema(cls):
        """
        Obtain the json schema for a kronos event
        """

        with open(cls.schema_json, 'r') as fschema:
            return json.load(fschema)

    @classmethod
    def validate_json(cls, js):
        """
        Do validation of a dictionary that has been loaded from (or will be written to) a JSON
        """
        jsonschema.validate(js, cls.schema(), format_checker=jsonschema.FormatChecker())
