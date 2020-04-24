#!/usr/bin/env python3

# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""

Simple utility tool that can be used to send an "end-of-job" notification 
message to the Kronos executor. This tool is used to run applications other 
than synthetic applications as part of the Kronos time_schedule. In fact, if an
arbitrary application is submitted as part of the time_schedule, the Kronos-executor
needs to know when the applications has completed and therefore this tool
is invoked straight after the invokation of the application executable in the 
job submit script. This tool is not needed in the job submit script of 
synthetic applications as they will already send an "end-of-job" message 
at the end of the job.

Example of usage (within the job submit script, after invokation
of the application executable):

source <kronos-installer>/environment.sh
export KRONOS_TOKEN="<kronos-simulation-unique-token>"
python send_job_complete_msg.py <host-name> <host-port> <job-id>

NOTE-1: the KRONOS_TOKEN is a unique simulation identifier and the Kronos
executor will only accept messages that contain a matching token. It can 
be found in the kronos-executor.log file (together with host-name and port).

NOTE-2: this script can also be used to manually "test" the correct reception 
of messages by the kronos executor while the executor is running (e.g.
sending the executor dummy "job-id" values, the reception of the message 
should be shown on STDOUT and later in the kronos-executor.log.
"""

import socket
import json
import time
import sys
import os


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def send_event(server_host, server_port, message):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (server_host, server_port)
        sock.connect(server_address)
        _error = sock.sendall(message.encode('ascii'))

        if not _error:
            eprint("message sent correctly: {}".format(message))
        else:
            eprint("ERROR in sending message!!")

    finally:
        sock.close()


if __name__ == "__main__":

    server_host = sys.argv[1] 
    server_port = int(sys.argv[2])
    job_id = sys.argv[3] 

    final = {"info": {
                "timestamp": int(time.time()),
                "job": int(job_id),
                "app": "kronos-synapp"
             },
             "type": "Complete",
             "token": os.environ.get("KRONOS_TOKEN", "UNKNOWN")
             }

    send_event(server_host, server_port, json.dumps(final))
