# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.executor.kronos_events.event_base import EventBase


class EventMetadataChange(EventBase):
    """
    An event that communicates a metadata change
    """

    def __init__(self, event_json, validate_event=False):
        super(EventMetadataChange, self).__init__(event_json, validate_event=validate_event)

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

        if self.metadata["step"] != other.metadata["step"]:
            return False

        return True