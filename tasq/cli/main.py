# -*- coding: utf-8 -*-

"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
from ..settings import get_config


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--addr', '-a', action='store')
    parser.add_argument('--port', '-p', action='store')
    return parser


def start_worker(host, port):
    from tasq.remote.master import Master
    master = Master(host, port, port + 1, debug=True)
    master.serve_forever()


def main():
    conf = get_config()
    parser = get_parser()
    args = parser.parse_args()
    host, port = conf['host'], conf['port']
    if args.worker:
        if args.addr:
            host = args.addr
        if args.port:
            port = int(args.port)
        start_worker(host, port)
