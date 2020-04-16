# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos_executor.kronos_events.event_base import EventBase


class EventMetadataChange(EventBase):
    """
    An event that communicates a metadata change
    """

    def __init__(self, event_json, validate_event=False):
        super(EventMetadataChange, self).__init__(event_json, validate_event=validate_event)

    def get_hashed(self):
        """
        Hashed version of this event
        :return:
        """

        # create a hashable version of this event
        return tuple((
                     ("type", self.type),
                     ("app", self.info["app"]),
                     ("job", self.info["job"]),
                     tuple(tuple((k, v)) for k, v in sorted(self.metadata.items()) ),
                     )
                    )

    def __unicode__(self):
        return "KRONOS-EVENT: type:{}; job:{}; metadata::{}".format(self.type,
                                                                   self.info.get("job"),
                                                                   ", ".join(["{}:{}".format(k, v) for k, v in self.metadata.items()])
                                                                   )

    def __eq__(self, other):
        """
        Check for equality of metadata change events
        :param other:
        :return:
        """

        if self.type != other.type:
            return False

        if self.info["job"] != other.info["job"]:
            return False

        # Check that all the metadata match (keys and values)
        if not all(k1 == k2 for k1, k2 in zip(sorted(self.metadata.keys()), sorted(other.metadata.keys()))):
            return False

        for k in self.metadata.keys():
            if self.metadata[k] != other.metadata[k]:
                return False

        return True
