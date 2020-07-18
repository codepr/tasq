"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

import argparse
from ..logger import logger, get_logger
from ..settings import get_config
from ..remote.runner import runner_factory
from ..exception import UnknownRunnerException


log = get_logger(__name__)


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
    parser.add_argument(
        "runner_type",
        nargs="?",
        default="runner",
        choices=["runner", "redis-runner", "rabbitmq-runner"],
        help="Set runner type, can be one between [runner | redis-runner | rabbitmq-runner]",
    )
    parser.add_argument(
        "--conf", "-c", help="The filepath to the configuration file, in json",
    )
    parser.add_argument(
        "--address",
        "-a",
        help="The ZMQ host address to connect to, default to localhost",
    )
    parser.add_argument(
        "--port",
        "-p",
        help="The ZMQ port to connect to, default to 9000 for "
        "ZMQ/TCP/UNIX connections, to 6379 while using a "
        "redis broker or 5672 in case of RabbitMQ as backend.",
        type=int,
    )
    parser.add_argument(
        "--pull-port",
        help="The ZMQ port to connect to, default to 9001 for "
        "ZMQ/TCP/UNIX connections",
        type=int,
    )
    parser.add_argument(
        "--worker-type", help="The type of worker to deploy for a runner"
    )
    parser.add_argument(
        "--db", help="The database to use with redis as backend", type=int,
    )
    parser.add_argument(
        "--name",
        help="The name of the queue, only for redis or rabbitmq backends",
    )
    parser.add_argument(
        "--signkey",
        help="The shared key to use to sign byte streams between clients and "
        "runners",
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
        type=int,
    )
    parser.add_argument("--log-level", help="Set logging level")
    args = parser.parse_args()
    return args


def start_worker(runner_type, worker_type, host, **kwargs):
    try:
        s_type = runners[worker_type][runner_type]
    except KeyError:
        raise UnknownRunnerException()
    try:
        log.info(
            "Starting %s runner type with %s worker type",
            runner_type.upper(),
            worker_type.upper(),
        )
        runner = runner_factory.create(s_type, host=host, **kwargs)
        runner.start()
    except KeyboardInterrupt:
        runner.stop()


def main():
    args = parse_arguments()
    conf = get_config(args.conf)
    runner_type = args.runner_type or "runner"
    logger.loglevel = args.log_level or conf["log_level"]
    signkey = args.signkey or conf["signkey"]
    unix = conf["unix"]
    num_workers = args.num_workers or conf["num_workers"]
    worker_type = args.worker_type or "actor"
    addr = args.address or conf["addr"]
    if runner_type == "runner":
        push_port = args.pull_port or conf["zmq"]["push_port"]
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
    elif runner_type == "redis-runner":
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
    elif runner_type == "rabbitmq-runner":
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
            role="receiver",
        )
