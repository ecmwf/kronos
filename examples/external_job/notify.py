
from __future__ import print_function

import argparse
import socket
import json
import sys
import os


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def send_event(server_host, server_port, message):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (server_host, server_port)
        sock.connect(server_address)
        _error = sock.sendall(message)

        if not _error:
            eprint("message sent correctly: {}".format(message))
        else:
            eprint("ERROR in sending message!!")

    finally:
        sock.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('server_host', type=str)
    parser.add_argument('server_port', type=int)
    parser.add_argument('job_id', type=int)
    parser.add_argument('app_id', type=str)
    parser.add_argument('metadata', type=str, default=None, nargs='?')

    parser.add_argument('-t', '--type', choices=['Complete', 'MetadataChange', 'NotifyMetadata'], required=True)

    args = parser.parse_args()

    message = {"info": {
                "job": args.job_id,
                "app": args.app_id,
             },
             "type": args.type,
             "token": os.environ.get("KRONOS_TOKEN", "UNKNOWN")
             }

    if args.type in ['MetadataChange', 'NotifyMetadata']:
        assert args.metadata is not None
        message["metadata"] = json.loads(args.metadata)

    send_event(args.server_host, args.server_port, json.dumps(message))
