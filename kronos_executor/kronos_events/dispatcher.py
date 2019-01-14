# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import Queue
import logging
import multiprocessing
import socket
from datetime import datetime

from kronos_modeller.tools.shared_utils import datetime2epochs

from kronos_executor.kronos_events import EventFactory

logger = logging.getLogger(__name__)


class EventDispatcher(object):

    """
    Class that dispatches Kronos events
    """

    buffer_size = 4096

    def __init__(self, queue, server_host='localhost', server_port=7363, sim_token=None):

        """
        Setup the socket and bind it to the appropriate port
        """

        # full address of the server
        self.server_host = server_host
        self.server_port = server_port
        self.server_address = (server_host, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # bind it to port and set it to listen status
        self.sock.bind(self.server_address)
        self.sock.listen(1024)
        self.terminate = False

        # Unique simulation hash
        self.sim_token = sim_token

        # listener process
        self.listener_queue = queue
        self.listener_proc = multiprocessing.Process(target=self._listen_for_messages)

        # log of all events with timings taken upon msg reception
        self.timed_events = []

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

                        # store event and timestamp in the queue (and wait until there is space in the queue)
                        self.listener_queue.put((msg, datetime2epochs(datetime.now())), block=True)

                        break

            finally:

                # ..and close the connection
                connection.close()

    def get_next_message(self):
        return self.listener_queue.get()

    def get_events_batch(self, batch_size=1):
        """
        Get a batch of events
        :param batch_size:
        :return:
        """

        _batch = []

        queue_empty_reached = False
        try:
            while len(_batch) < batch_size:

                # get msg and timestamp from the queue
                (msg, msg_timestamp) = self.listener_queue.get(block=False)

                kronos_event = EventFactory.from_string(msg, validate_event=False)

                if kronos_event:

                    # Dispatch only legitimate messages (i.e. TOKEN present and correct)
                    if hasattr(kronos_event, "token"):
                        if str(kronos_event.token) == str(self.sim_token):
                            _batch.append(kronos_event)
                            self.timed_events.append((kronos_event, msg_timestamp))
                        else:
                            logger.warning("INCORRECT TOKEN {} => message discarded: {}".format(kronos_event.token, msg))
                    else:

                        # one last attempt to find the token
                        # TODO: ideally we would keep the check of the token at the top level only..
                        if hasattr(kronos_event, "info"):
                            if kronos_event.info.get("token", "") == str(self.sim_token):
                                _batch.append(kronos_event)
                                self.timed_events.append((kronos_event, msg_timestamp))
                            else:
                                logger.warning("TOKEN NOT found => message discarded: {}".format(msg))
                        else:
                            logger.warning("TOKEN NOT found => message discarded: {}".format(msg))

        except Queue.Empty:
            queue_empty_reached = True
            pass

        return queue_empty_reached, _batch

    def stop(self):
        self.listener_proc.terminate()
