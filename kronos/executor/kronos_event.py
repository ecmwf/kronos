# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import json
import jsonschema
import os


class KronosEvent(object):

    """
    A Kronos event that can be triggered by jobs
    """

    # JSON schema of a kronos event
    schema_json = os.path.join(os.path.dirname(__file__), "event_schema.json")

    def __init__(self, message_json):

        # keeps the json internally
        self.__message_json = message_json

        # check all the configurations
        self.validate_json(message_json)

        # keep the top-level keys as attributes
        for k, v in message_json.iteritems():
            setattr(self, k, v)

    def __unicode__(self):
        return "KRONOS-EVENT:\n{}".format(self.__message_json)

    def __str__(self):
        return unicode(self).encode('utf-8')

    @classmethod
    def from_timestamp(cls, timestamp):
        """
        Just a timer event from a simple timestamp
        :param timestamp:
        :return:
        """

        event_json = {
            "type": "timer",
            "info": {
                "timestamp": timestamp
            }
        }

        return cls(event_json)

    @classmethod
    def from_json(cls, event_json):
        """
        An event directly from a json structure
        :param event_json:
        :return:
        """

        return cls(event_json)

    @classmethod
    def from_string(cls, event_string):
        """
        An event from a string (typically received through the network)
        :param event_string:
        :return:
        """

        if not len(event_string):
            return

        # decode the string into a json..
        try:
            event_json = json.loads(event_string)
        except ValueError:
            event_json = json.loads(event_string[:-1])

        return cls(event_json)

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
        # print "validating message: ", js
        jsonschema.validate(js, cls.schema(), format_checker=jsonschema.FormatChecker())
