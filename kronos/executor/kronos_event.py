# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import json


class KronosEvent(object):

    """
    A Kronos event that can be triggered by jobs
    """

    accepted_keys = {
        "app": {"key_type": str},
        "event": {"key_type": str, "choices": ["complete", "time", "metadata-changes"]},
        "timestamp": {"key_type": float},
        "job_id": {"key_type": int}
    }

    def __init__(self, message):

        # check that the message is well formed and decode it
        self.decode_message(message)

    def __unicode__(self):
        return "KRONOS-EVENT:\n{}".format("\n".join(["-- {}: {}".format(k, getattr(self, k))
                                                     for k, v in self.accepted_keys.iteritems()]))

    def __str__(self):
        return unicode(self).encode('utf-8')

    @classmethod
    def from_time(cls, timestamp):

        message = {
            "app": "unknown",
            "event": "time",
            "timestamp": timestamp,
            "job_id": -1
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
        for k, v in message_json.iteritems():

            # check keys against allowed keys
            if k not in self.accepted_keys:
                raise ValueError("Unexpected configuration keyword provided - {}:{}".format(k, v))

            # check values against valid choices (if any)
            if self.accepted_keys[k].get("choices"):
                assert v in self.accepted_keys[k]["choices"]

            # finally add all the attributes from the decoded message
            if self.accepted_keys[k].get("key_type"):
                setattr(self, k, self.accepted_keys[k]["key_type"](v))
            else:
                setattr(self, k, v)
