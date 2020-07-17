"""
tasq.settings.py
~~~~~~~~~~~~~~~~
"""
import os
from .util import Configuration


class TasqConfig(Configuration):

    defaults = {
        "addr": "127.0.0.1",
        "zmq": {"push_port": 9001, "pull_port": 9000},
        "redis": {"port": 6379, "db": 0, "name": "redis-queue"},
        "rabbitmq": {"port": 5672, "name": "amqp-queue"},
        "signkey": os.getenv("TASQ_SIGN_KEY", None),
        "unix": False,
        "log_level": "INFO",
        "num_workers": 4,
    }


def get_config(path=os.getenv("TASQ_CONF", "~/.tasq/configuration.json")):
    rc = TasqConfig(path)
    return rc
