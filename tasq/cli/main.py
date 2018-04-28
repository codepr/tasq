# -*- coding: utf-8 -*-

"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--remote', action='store_true')
    parser.add_argument('--addr', '-a', action='store')
    parser.add_argument('--port', '-p', action='store')
    return parser


def start_local_workers():
    pass


def start_remote_workers(host, port):
    from tasq.remote.remoteworker import RemoteWorker
    rw = RemoteWorker(host, port, port + 1, debug=True)
    try:
        rw.start()
    except KeyboardInterrupt:
        print("\nStopping workers..")


def main():
    parser = get_parser()
    args = parser.parse_args()
    host, port = '127.0.0.1', 9000
    if args.local:
        start_local_workers()
    else:
        if args.addr:
            host = args.addr
        if args.port:
            port = args.port
        start_remote_workers(host, port)
