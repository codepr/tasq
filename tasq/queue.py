"""
tasq.queue.py
~~~~~~~~~~~~~
The main client module, provides interfaces to instantiate queues
"""

import os
from urllib.parse import urlparse
from tasq.remote.backends.redis import RedisStore
from tasq.remote.client import ZMQTasqClient, RedisTasqClient, RabbitMQTasqClient


class TasqQueue:

    """Main queue abstraction, accept a backend and a store as well as nothing
    fallbacking in the last case to the ZMQ client.

    The formats accepted for the backends are:

    - redis://localhost:6379/0?name=test-queue-redis
    - amqp://localhost:5672?name=test-queue-rabbitmq
    - zmq://localhost:9000?pull_port=9010
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

        def get_client(protocol, host=None, port=None, *args, **kwargs):
            if protocol == 'redis':
                return RedisTasqClient(host or 'localhost',
                                       port or 6379,
                                       *args, **kwargs)
            if protocol == 'amqp':
                return RabbitMQTasqClient(host or 'localhost',
                                          port or 5672,
                                          *args, **kwargs)
            if protocol == 'unix':
                return ZMQTasqClient(host or 'localhost',
                                     port or 9000,
                                     unix_socket=True,
                                     *args, *kwargs)
            return ZMQTasqClient(host or 'localhost',
                                 port or 9000,
                                 *args, **kwargs)

        url = urlparse(backend)
        assert url.scheme in {'redis', 'zmq', 'amqp', 'unix', 'tcp'}, \
            f"Unsupported {url.scheme}"
        scheme, addr, port = url.scheme, url.hostname, url.port
        if scheme == 'redis':
            db = int(url.path.split('/')[-1]) if url.path else 0
            name = url.query.split('=')[-1] if url.query else f'{os.getpid()}'
            self._backend = get_client(scheme, addr,
                                       port, db=db,
                                       name=name, sign_data=sign_data)
        elif scheme == 'amqp':
            name = url.query.split('=')[-1] if url.query else f'{os.getpid()}'
            self._backend = get_client(scheme, addr, port,
                                       name=name, sign_data=sign_data)
        else:
            pull_port = int(url.query.split('=')[-1]) if url.query else None
            self._backend = get_client(scheme, addr, port,
                                       plport=pull_port, sign_data=sign_data)

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
