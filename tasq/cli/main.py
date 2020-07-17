"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

import argparse
from ..logger import logger
from ..settings import get_config
from ..remote.runner import runner_factory


class UnknownRunnerException(Exception):
    pass


runners = {
    "actor": {
        "zmq": "ZMQ_ACTOR_RUNNER",
        "redis": "REDIS_ACTOR_RUNNER",
        "rabbitmq": "AMQP_ACTOR_RUNNER",
    },
    "process": {
        "zmq": "ZMQ_QUEUE_RUNNER",
        "redis": "REDIS_QUEUE_RUNNER",
        "rabbitmq": "AMQP_QUEUE_RUNNER",
    },
}


def parse_arguments():
    parser = argparse.ArgumentParser(description="Tasq CLI")
    parser.add_argument("subcommand")
    parser.add_argument(
        "--conf",
        "-c",
        help="The filepath to the configuration file, in json",
        nargs="?",
    )
    parser.add_argument(
        "--address",
        "-a",
        help="The ZMQ host address to connect to, default to localhost",
        nargs="?",
    )
    parser.add_argument(
        "--port",
        "-p",
        help="The ZMQ port to connect to, default to 9000 for "
        "ZMQ/TCP/UNIX connections, to 6379 while using a "
        "redis broker or 5672 in case of RabbitMQ as backend.",
        nargs="?",
        type=int,
    )
    parser.add_argument(
        "--plport",
        help="The ZMQ port to connect to, default to 9001 for "
        "ZMQ/TCP/UNIX connections",
        nargs="?",
        type=int,
    )
    parser.add_argument(
        "--worker-type",
        help="The type of worker to deploy for a runner",
        nargs="?",
    )
    parser.add_argument(
        "--db",
        help="The database to use with redis as backend",
        nargs="?",
        type=int,
    )
    parser.add_argument(
        "--name",
        help="The name of the queue, only for redis or rabbitmq backends",
        nargs="?",
    )
    parser.add_argument(
        "--signkey",
        help="The shared key to use to sign byte streams between clients and "
        "runners",
        nargs="?",
    )
    parser.add_argument(
        "--unix",
        "-u",
        help="Unix socket flag, in case runners and "
        "clients reside on the same node",
        action="store_true",
    )
    parser.add_argument(
        "--num-workers",
        help="Number of workers to instantiate on the node",
        nargs="?",
        type=int,
    )
    parser.add_argument("--log-level", help="Set logging level", nargs="?")
    args = parser.parse_args()
    return args


def start_worker(runner_type, worker_type, host, **kwargs):
    try:
        s_type = runners[worker_type][runner_type]
    except KeyError:
        raise UnknownRunnerException()
    try:
        runner = runner_factory.create(s_type, host=host, **kwargs)
        runner.start()
    except KeyboardInterrupt:
        runner.stop()


def main():
    args = parse_arguments()
    conf = get_config(args.conf)
    logger.loglevel = args.log_level or conf["log_level"]
    signkey = args.signkey or conf["signkey"]
    unix = conf["unix"]
    num_workers = args.num_workers or conf["num_workers"]
    worker_type = args.worker_type or "actor"
    addr = args.address or conf["addr"]
    if args.subcommand == "worker":
        push_port = args.plport or conf["zmq"]["push_port"]
        pull_port = args.port or conf["zmq"]["pull_port"]
        start_worker(
            "zmq",
            worker_type,
            addr,
            channel=(push_port, pull_port),
            signkey=signkey,
            num_workers=num_workers,
            unix=unix,
        )
    elif args.subcommand == "redis-worker":
        port = args.port or conf["redis"]["port"]
        db = args.db or conf["redis"]["db"]
        name = args.name or conf["redis"]["name"]
        start_worker(
            "redis",
            worker_type,
            addr,
            port=port,
            db=db,
            name=name,
            num_workers=num_workers,
            signkey=signkey,
        )
    elif args.subcommand == "rabbitmq-worker":
        port = args.port or conf["rabbitmq"]["port"]
        name = args.name or conf["rabbitmq"]["name"]
        start_worker(
            "rabbitmq",
            worker_type,
            addr,
            port=port,
            name=name,
            num_workers=num_workers,
            signkey=signkey,
        )
