import json
import logging

from kronos.executor.kronos_events.event_complete import EventComplete
from kronos.executor.kronos_events.event_metadatachange import EventMetadataChange
from kronos.executor.kronos_events.event_time import EventTime

event_types = {
    "MetadataChange": EventMetadataChange,
    "Time": EventTime,
    "Complete": EventComplete
}

logger = logging.getLogger(__name__)


class KronosEventException(Exception):
    pass


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

        if not event_json.get("type"):
            logger.warning("Event instantiation FAILED. 'type' key missing in {}".format(event_json))
            return None

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
            return None

        # try to decode the string into a json..
        try:
            event_json = json.loads(event_string)
        except ValueError:
            try:
                event_json = json.loads(event_string[:-1])
            except ValueError:
                logger.warning("Event decoding FAILED => message discarded {}".format(event_string))
                return None

        if isinstance(event_json, unicode):
            logger.warning("Event decoding FAILED => message discarded {}".format(event_string))
            return None

        if not event_json.get("type"):
            logger.warning("Event parsing FAILED (missing 'type') => message discarded {}".format(event_json))
            return None

        return event_types[event_json["type"]](event_json, validate_event=validate_event)
