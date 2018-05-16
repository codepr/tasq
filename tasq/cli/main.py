# -*- coding: utf-8 -*-

"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
from ..settings import get_config


def get_parser():
    parser = argparse.ArgumentParser(description='Tasq CLI commands')
    parser.add_argument('subcommand')
    parser.add_argument('-f', action='store')
    parser.add_argument('--secure', '-s', action='store_true')
    parser.add_argument('--workers', nargs='*')
    parser.add_argument('--random', action='store')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--addr', '-a', action='store')
    parser.add_argument('--port', '-p', action='store')
    return parser


def start_worker(host, port, debug, sign_data):
    from tasq.remote.master import Master
    master = Master(host, port, port + 1, debug=debug, sign_data=sign_data)
    master.serve_forever()


def start_workers(workers, debug, sign_data):
    from tasq.remote.master import Masters
    masters = Masters(workers, debug=debug)
    masters.start_procs()


def start_random_workers(host, num_workers, debug, sign_data):
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
    masters = Masters(list(workers_set), debug=debug)
    masters.start_procs()


def _translate_peers(workers):
    return [tuple(x.split(':')) for x in workers]


def main():
    conf = get_config()
    parser = get_parser()
    args = parser.parse_args()
    if args.f:
        conf = get_config(path=args.f)
    host, port = conf['host'], conf['port']
    debug = conf['debug']
    sign_data = conf['sign_data']
    if args.debug:
        debug = True
    if args.secure:
        sign_data = True
    if args.workers:
        try:
            pairs = conf['workers']
        except KeyError:
            pairs = args.workers
            if not pairs:
                print("No [host:port] list specified")
            else:
                workers = _translate_peers(pairs)
        else:
            workers = _translate_peers(args.workers or conf['workers'])
        start_workers(workers, debug, sign_data)
    if args.subcommand == 'worker':
        if args.addr:
            host = args.addr
        if args.port:
            port = int(args.port)
        start_worker(host, port, debug, sign_data)
    elif args.random:
        start_random_workers(host, int(args.random), debug, sign_data)
