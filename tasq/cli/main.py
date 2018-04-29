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
    parser.add_argument('--random', action='store_true')
    parser.add_argument('--addr', '-a', action='store')
    parser.add_argument('--port', '-p', action='store')
    return parser


def start_worker(host, port):
    from tasq.remote.master import Master
    master = Master(host, port, port + 1, debug=True)
    master.serve_forever()


def start_random_workers(host, num_workers):
    import random
    from tasq.remote.master import Masters
    workers_set = set()
    init_port = 9000
    while True:
        port = random.randint(init_port, 65000)
        if (host, port, port + 1) in workers_set:
            continue
        workers_set.add((host, port, port + 1))
        if len(workers_set) == num_workers:
            break
        init_port = port + 2
    masters = Masters(list(workers_set), debug=True)
    masters.start_procs()


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
    elif args.random:
        start_random_workers(host, 5)
