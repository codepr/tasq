"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import argparse
from enum import Enum
from ..settings import get_config


class WorkerType(Enum):
    ActorWorker = 'actor'
    ProcessWorker = 'process'


def get_parser():
    parser = argparse.ArgumentParser(description='Tasq CLI commands')
    parser.add_argument('subcommand')
    parser.add_argument('-f', action='store')
    parser.add_argument('--secure', '-s', action='store_true')
    parser.add_argument('--unix', '-u', action='store_true')
    parser.add_argument('--workers', nargs='*')
    parser.add_argument('--worker-type', action='store')
    parser.add_argument('--num-workers', action='store')
    parser.add_argument('--random', action='store')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--addr', '-a', action='store')
    parser.add_argument('--port', '-p', action='store')
    parser.add_argument('--db', action='store')
    parser.add_argument('--name', action='store')
    return parser


def start_worker(host, port, sign_data, unix_socket, worker_type):
    from tasq.remote.supervisor import ZMQActorSupervisor, ZMQQueueSupervisor
    if worker_type == WorkerType.ActorWorker:
        supervisor = ZMQActorSupervisor(host, port, port + 1,
                                sign_data=sign_data, unix_socket=unix_socket)
    else:
        supervisor = ZMQQueueSupervisor(host, port, port + 1,
                                sign_data=sign_data, unix_socket=unix_socket)
    supervisor.serve_forever()


def start_redis_worker(host, port, db, name, sign_data, worker_type):
    from tasq.remote.supervisor import RedisActorSupervisor, RedisQueueSupervisor
    if worker_type == WorkerType.ActorWorker:
        supervisor = RedisActorSupervisor(host, port, db,
                                          name, sign_data=sign_data)
    else:
        supervisor = RedisQueueSupervisor(host, port, db, name,
                                          sign_data=sign_data)
    supervisor.serve_forever()


def start_rabbitmq_worker(host, port, name, sign_data, worker_type):
    from tasq.remote.supervisor import RabbitMQActorSupervisor, RabbitMQQueueSupervisor
    if worker_type == WorkerType.ActorWorker:
        supervisor = RabbitMQActorSupervisor(host, port,
                                          name, sign_data=sign_data)
    else:
        supervisor = RabbitMQQueueSupervisor(host, port, name,
                                          sign_data=sign_data)
    supervisor.serve_forever()


def start_workers(workers, sign_data, unix_socket):
    from tasq.remote.supervisor import Supervisors
    supervisors = Supervisors(workers, sign_data=sign_data, unix_socket=unix_socket)
    supervisors.start_procs()


def start_random_workers(host, num_workers, sign_data, unix_socket):
    import random
    from tasq.remote.supervisor import Supervisors
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
    supervisors = Supervisors(list(workers_set), sign_data=sign_data, unix_socket=unix_socket)
    supervisors.start_procs()


def _translate_peers(workers):
    return [tuple(x.split(':')) for x in workers]


def main():
    conf = get_config()
    parser = get_parser()
    args = parser.parse_args()
    if args.f:
        conf = get_config(path=args.f)
    host, port = conf['host'], conf['port']
    verbose = conf['verbose']
    sign_data = conf['sign_data']
    unix_socket = conf['unix_socket']
    num_workers = 4
    db = 0
    worker_type = WorkerType.ActorWorker
    if args.verbose:
        verbose = True
    if args.secure:
        sign_data = True
    if args.unix:
        unix_socket = True
    if args.num_workers:
        num_workers = args.num_workers
    if args.worker_type:
        try:
            worker_type = WorkerType(args.worker_type)
        except ValueError:
            print(f"{args.worker_type} is not a valid type: use either process "
                  "or actor.  Fallbacking to actor")
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
        start_workers(workers, sign_data, unix_socket)
    if args.subcommand == 'worker':
        if args.addr:
            host = args.addr
        if args.port:
            port = int(args.port)
        start_worker(host, port, sign_data, unix_socket, worker_type)
    elif args.subcommand == 'redis':
        if args.addr:
            host = args.addr
        port = 6379
        if args.port:
            port = int(args.port)
        if args.db:
            db = int(args.db)
        name = args.name or 'redis-queue'
        start_redis_worker(host, port, db, name, sign_data, worker_type)
    elif args.subcommand == 'rabbitmq':
        if args.addr:
            host = args.addr
        port = 5672
        if args.port:
            port = int(args.port)
        name = args.name or 'rabbitmq-queue'
        start_rabbitmq_worker(host, port, name, sign_data, worker_type)
    elif args.random:
        start_random_workers(host, int(args.random), sign_data, unix_socket)
