"""
tasq.queue.py
~~~~~~~~~~~~~
The main client module, provides interfaces to instantiate queues
"""

import os
from urllib.parse import urlparse
from collections import namedtuple
from tasq.remote.backends.redis import RedisStore
from tasq.remote.client import (ZMQTasqClient, RedisTasqClient,
                                RabbitMQTasqClient)


def init_client(client, host, port, *args, **kwargs):
    return client(host, port, *args, **kwargs)


Client = namedtuple('Client', ('handler', 'arguments'))


defaults = {
    'redis': Client(RedisTasqClient, {'host': '127.0.0.1',
                                      'port': 6379,
                                      'db': 0,
                                      'name': os.getpid()}),
    'amqp': Client(RabbitMQTasqClient, {'host': '127.0.0.1',
                                        'port': 5672,
                                        'name': os.getpid()}),
    'unix': Client(ZMQTasqClient, {'host': '127.0.0.1',
                                   'port': 9000,
                                   'plport': 9001,
                                   'unix_socket': True}),
    'zmq': Client(ZMQTasqClient, {'host': '127.0.0.1',
                                  'port': 9000,
                                  'plport': 9001}),
    'tcp': Client(ZMQTasqClient, {'host': '127.0.0.1',
                                  'port': 9000,
                                  'plport': 9001})
}


class TasqQueue:

    """Main queue abstraction, accept a backend and a store as well as nothing
    fallbacking in the last case to the ZMQ client.

    The formats accepted for the backends are:

    - redis://localhost:6379/0?name=test-queue-redis
    - amqp://localhost:5672?name=test-queue-rabbitmq
    - zmq://localhost:9000?plport=9010
    - tcp://localhost:5555

    For the store part currently only Redis is supported as a backend:

    - redis://localhost:6379/1?name=results-store

    Attributes
    ----------
    :type backend: str or u'zmq://localhost:9000'
    :param backend: An URL to connect to for the backend service

    :type store: str or None
    :param store: An URL to connect to for the results store service

    :type sign_data: bool or False
    :param sign_data: A boolean flag, sign data with a shared key

    """

    def __init__(self, backend=u'zmq://localhost:9000',
                 store=None, sign_data=False):

        url = urlparse(backend)
        scheme = url.scheme or 'zmq'
        assert url.scheme in {'redis', 'zmq', 'amqp', 'unix', 'tcp'}, \
            f"Unsupported {url.scheme}"
        args = {
            'host': url.hostname,
            'port': url.port,
            'db': int(url.path.split('/')[-1]) if url.path else None,
            'name': url.query.split('=')[-1] if url.query and scheme not in {'tcp', 'zmq'} else None,
            'plport': int(url.query.split('=')[-1]) if url.query and scheme == 'zmq' else None,
            'sign_data': sign_data
        }

        # Update defaults arguments
        for k in defaults[scheme].arguments:
            if k not in args or not args[k]:
                args[k] = defaults[scheme].arguments[k]

        # Remove useless args
        args = {k: v for k, v in args.items() if v is not None}

        self._backend = defaults[scheme].handler(**args)

        if store:
            urlstore = urlparse(store)
            assert urlstore.scheme in {'redis'}, f"Unknown {scheme}"
            db = int(urlstore.path.split('/')[-1]) if url.query else 0
            self._store = RedisStore(urlstore.hostname, urlstore.port, db)
        else:
            self._store = store
        # Connect with the backend
        self._backend.connect()

    def put(self, func, *args, **kwargs):
        return self._backend.schedule(func, *args, **kwargs)

    def pub_blocking(self, func, *args, **kwargs):
        return self._backend.schedule_blocking(func, *args, **kwargs)

    def pending_jobs(self):
        return self._backend.pending_jobs()

    def results(self):
        return self._backend.results
