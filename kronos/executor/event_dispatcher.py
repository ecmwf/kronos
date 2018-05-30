# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import socket

from kronos.executor.kronos_event import KronosEvent


def dispatcher_callback(event_queue, host, port):

    # init the dispatcher
    hdl = EventDispatcher(event_queue, server_host=host, server_port=port)

    # get messages as they come, until terminate_signal signal is True
    hdl.handle_incoming_messages()


class EventDispatcher(object):

    """
    Class that dispatches Kronos events
    """

    buffer_size = 4096

    def __init__(self, event_queue, server_host='localhost', server_port=7363):

        """
        Setup the socket and bind it to the appropriate port
        """

        # full address of the server
        self.server_address = (server_host, server_port)
        print "self.server_address ", self.server_address

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print 'starting up on %s port %s' % self.server_address

        # bind it to port and set it to listen status
        self.sock.bind(self.server_address)
        self.sock.listen(1)
        self.events = []

        self.event_queue = event_queue
        self.terminate = False

    def __unicode__(self):

        return "KRONOS-HANDLER:\n{}".format("\n".join(["-- "+e.__str__() for e in self.events]))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def handle_incoming_messages(self):
        """
        Handles the incoming TCP events
        :return:
        """

        print "ready for listening.."

        # keep accepting connections until all the jobs have completed
        while True:

            connection, client_address = self.sock.accept()

            try:
                msg = ""

                # keep looping until there is data to be received..
                while True:
                    data = connection.recv(self.buffer_size)
                    if data:
                        msg += data
                    else:
                        break

            finally:

                # add this event to the list of events to be handled..
                kronos_event = KronosEvent(msg)

                print "got event! {}".format(kronos_event)

                # store internally the full list of events
                self.events.append(kronos_event)

                # put this events in the queue, ready for dispatch
                self.event_queue.put_nowait(self.events)

                # ..and close the connection
                connection.close()
