import json

from kronos.executor.kronos_events.event_complete import EventComplete
from kronos.executor.kronos_events.event_metadatachange import EventMetadataChange
from kronos.executor.kronos_events.event_time import EventTime

event_types = {
    "MetaDataChange": EventMetadataChange,
    "Time": EventTime,
    "Complete": EventComplete
}


class EventFactory(object):

    @staticmethod
    def from_timestamp(timestamp):
        """
        Just a timer event from a simple timestamp
        :param timestamp:
        :return:
        """

        event_json = {
            "type": "Time",
            "info": {
                "timestamp": timestamp
            }
        }

        return event_types[event_json["type"]](event_json)

    @staticmethod
    def from_dictionary(event_json):
        """
        An event directly from a json structure
        :param event_json:
        :return:
        """

        return event_types[event_json["type"]](event_json)

    @staticmethod
    def from_string(event_string):
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

        return event_types[event_json["type"]](event_json)
