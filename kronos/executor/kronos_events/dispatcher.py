# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import socket
import multiprocessing
import Queue

from kronos.executor.kronos_events import EventFactory


class EventDispatcher(object):

    """
    Class that dispatches Kronos events
    """

    buffer_size = 4096

    def __init__(self, queue, server_host='localhost', server_port=7363):

        """
        Setup the socket and bind it to the appropriate port
        """

        # full address of the server
        self.server_host = server_host
        self.server_port = server_port
        self.server_address = (server_host, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print 'starting up on %s port %s' % self.server_address

        # bind it to port and set it to listen status
        self.sock.bind(self.server_address)
        self.sock.listen(10)
        self.terminate = False

        # listener process
        self.listener_queue = queue
        self.listener_proc = multiprocessing.Process(target=self._listen_for_messages)

    def __unicode__(self):
        return "KRONOS-DISPATCHER: host:{}, port:{} ".format(self.server_host, self.server_port)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def start(self):
        """
        Start the process
        :return:
        """
        self.listener_proc.start()

    def _listen_for_messages(self):
        """
        Handles the incoming TCP events
        :return:
        """

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

                        # with open("dispatched_events.log", "a") as myfile:
                        #     myfile.write("received msg: {}\n".format(msg))

                        # store event in the queue (and wait until there is space in the queue)
                        self.listener_queue.put(msg, block=True)

                        break

            finally:

                # # ..and close the connection
                connection.close()

    def get_next_message(self):
        return self.listener_queue.get()

    def get_events_batch(self, batch_size=1):
        """
        Get a batch of events
        :param batch_size:
        :return:
        """

        # _batch = [self.listener_queue.get(block=True)]
        _batch = []

        queue_empty_reached=False
        try:
            while len(_batch) < batch_size:

                msg = self.listener_queue.get(block=False)
                kronos_event = EventFactory.from_string(msg, validate_event=False)
                _batch.append(kronos_event)

        except Queue.Empty:
            queue_empty_reached=True
            pass

        return queue_empty_reached, _batch

    def stop(self):
        self.listener_proc.terminate()
