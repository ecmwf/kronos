# (C) Copyright 1996-2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from kronos_executor.kronos_events.event_base import EventBase


class EventFailed(EventBase):
    """
    An event that indicates a job has failed
    """

    def __init__(self, event_json, validate_event=False):
        super(EventFailed, self).__init__(event_json, validate_event=validate_event)

    def get_hashed(self):
        """
        Hashed version of this event
        :return:
        """

        # create a hashable version of this event
        return tuple((("type", self.type),
                     ("app", self.info["app"]),
                     ("job", self.info["job"]),
                     ))

    def __eq__(self, other):
        """
        Check for equality of metadata change events
        :param other:
        :return:
        """

        if self.type != other.type:
            return False

        if self.info["app"] != other.info["app"]:
            return False

        if self.info["job"] != other.info["job"]:
            return False

        return True
