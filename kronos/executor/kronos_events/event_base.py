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


class EventBase(object):

    """
    A Kronos event that can be triggered by jobs
    """

    # JSON schema of a kronos event
    schema_json = os.path.join(os.path.dirname(__file__), "schema.json")

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

    def __eq__(self, other_event):
        """
        Used to match: an event VS job-dependency event
        """

        raise NotImplementedError

    # @staticmethod
    # def from_timestamp(timestamp):
    #     """
    #     Just a timer event from a simple timestamp
    #     :param timestamp:
    #     :return:
    #     """
    #
    #     event_json = {
    #         "type": "Timer",
    #         "info": {
    #             "timestamp": timestamp
    #         }
    #     }
    #
    #     return event_types[event_json["type"]](event_json)
    #
    # @staticmethod
    # def from_json(event_json):
    #     """
    #     An event directly from a json structure
    #     :param event_json:
    #     :return:
    #     """
    #
    #     return event_types[event_json["type"]](event_json)
    #
    # @staticmethod
    # def from_string(event_string):
    #     """
    #     An event from a string (typically received through the network)
    #     :param event_string:
    #     :return:
    #     """
    #
    #     if not len(event_string):
    #         return
    #
    #     # decode the string into a json..
    #     try:
    #         event_json = json.loads(event_string)
    #     except ValueError:
    #         event_json = json.loads(event_string[:-1])
    #
    #     return event_types[event_json["type"]](event_json)

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
        print "validating message: ", js
        jsonschema.validate(js, cls.schema(), format_checker=jsonschema.FormatChecker())