# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.executor.kronos_events import EventFactory


class Manager(object):
    """
    A minimal manager of the simulation events
     - Stores the sequence of events passed in by the event dispatcher
     - It decides if a job is eligible for submission
    """

    def __init__(self, event_dispatcher):

        # uses an event dispatcher underneath
        self.dispatcher = event_dispatcher
        self.dispatcher.start()

        # list of events occurring during the simulation
        # categorised by event type (for efficiency)
        self.__events = {}

    def update_events(self, new_events):
        """
        Accept a new event
        :param new_event:
        :return:
        """

        for new_event in new_events:
            self.__events.setdefault(new_event.type, []).append(new_event)

    def get_events(self, type_filter=None):
        """
        All simulation events (or filtered by type)
        :return:
        """

        if type_filter:
            filtered_events = self.__events.get(type_filter, [])
        else:
            filtered_events = [ev for ev_type_list in self.__events.values() for ev in ev_type_list]

        return filtered_events

    def next_events(self, batch_size=1):
        """
        Wait until a message arrives from the dispatcher
        :return:
        """

        # get latest event from the dispatcher
        latest_events = self.dispatcher.get_events_batch(batch_size=batch_size)

        # update internal list of events as appropriate
        self.update_events(latest_events)

        # return the event
        return latest_events

    def next_event(self):
        """
        Wait until a message arrives from the dispatcher
        :return:
        """

        # get latest event from the dispatcher
        latest_event = self.dispatcher.get_next_message()

        # update internal list of events as appropriate
        self.update_events([latest_event])

        # return the event
        return latest_event

    def add_time_event(self, timestamp):
        """
        Add a timer event on request
        :param timestamp:
        :return:
        """
        self.__events.setdefault("Time", []).append(EventFactory.from_timestamp(timestamp))
