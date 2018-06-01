import json

from kronos.executor.kronos_events.event_complete import EventComplete
from kronos.executor.kronos_events.event_metadatachange import EventMetadataChange
from kronos.executor.kronos_events.event_time import EventTime

event_types = {
    "MetadataChange": EventMetadataChange,
    "Time": EventTime,
    "Complete": EventComplete
}


class EventFactory(object):

    @staticmethod
    def from_timestamp(timestamp, validate_event=False):
        """
        Just a timer event from a simple timestamp
        :param timestamp:
        :param validate_event:
        :return:
        """

        event_json = {
            "type": "Time",
            "info": {
                "timestamp": timestamp
            }
        }

        return event_types[event_json["type"]](event_json, validate_event=validate_event)

    @staticmethod
    def from_dictionary(event_json, validate_event=False):
        """
        An event directly from a json structure
        :param event_json:
        :param validate_event:
        :return:
        """

        return event_types[event_json["type"]](event_json, validate_event=validate_event)

    @staticmethod
    def from_string(event_string, validate_event=False):
        """
        An event from a string (typically received through the network)
        :param event_string:
        :param validate_event:
        :return:
        """

        if not len(event_string):
            return

        # decode the string into a json..
        try:
            event_json = json.loads(event_string)
        except ValueError:
            event_json = json.loads(event_string[:-1])

        return event_types[event_json["type"]](event_json, validate_event=validate_event)
