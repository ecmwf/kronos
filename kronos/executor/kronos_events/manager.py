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

        # list of events occurring during the simulation
        self.__events = []

    def update_events(self, new_event):
        """
        Accept a new event
        :param new_event:
        :return:
        """
        self.__events.append(new_event)

    def get_events(self, type_filter=None):
        """
        All simulation events (or filtered by type)
        :return:
        """

        if type_filter:
            filtered_events = [e for e in self.__events if e.type == type_filter]
        else:
            filtered_events = self.__events

        return filtered_events

    def wait_for_new_event(self):
        """
        Wait until a message arrives from the dispatcher
        :return:
        """

        self.update_events(self.dispatcher.get_next_message())

    def are_job_dependencies_fullfilled(self, job):
        """
        Returns if a job is ready for submission
        (i.e. all its dependencies events are fulfilled)
        :param job:
        :return:
        """

        return all(dep_ev in self.__events for dep_ev in job.depends)

    def add_time_event(self, timestamp):
        """
        Add a timer event on request
        :param timestamp:
        :return:
        """
        self.__events.append(EventFactory.from_timestamp(timestamp))



