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

    def __init__(self, message):

        # check that the message is well formed and decode it
        self.raw_message = message
        self.decode_message(message)

    def __unicode__(self):
        return "KRONOS-EVENT:\n{}".format(self.raw_message)

    def __str__(self):
        return unicode(self).encode('utf-8')

    @classmethod
    def from_time(cls, timestamp):

        message = {
            "type": "timer",
            "info": {
                "timestamp": timestamp
            }
        }

        return cls(json.dumps(message))

    def decode_message(self, message):
        """
        Decode the message
        :param message:
        :return:
        """

        if not len(message):
            return

        # decode the string into a json..
        try:
            message_json = json.loads(message)
        except ValueError:
            message_json = json.loads(message[:-1])

        # check all the configurations
        self.validate_json(message_json)

        # get the top-level keys as attributes
        for k, v in message_json.iteritems():
            setattr(self, k, v)

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
