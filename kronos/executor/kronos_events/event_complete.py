# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.executor.kronos_events.event_base import EventBase


class EventComplete(EventBase):
    """
    An event that communicates a "time"
    """

    def __init__(self, event_json):
        super(EventComplete, self).__init__(event_json)

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
