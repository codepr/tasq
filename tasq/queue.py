"""
tasq.queue.py
~~~~~~~~~~~~~
The main client module, provides interfaces to instantiate queues
"""

from queue import Queue
from threading import Thread
from urllib.parse import urlparse
from tasq.remote.backends.redis import RedisStore
from tasq.remote.client import (ZMQTasqClient, RedisTasqClient,
                                RabbitMQTasqClient, TasqFuture)


backends = {
    'redis': RedisTasqClient,
    'amqp': RabbitMQTasqClient,
    'unix': ZMQTasqClient,
    'zmq': ZMQTasqClient,
    'tcp': ZMQTasqClient
}


class TasqQueue:

    """Main queue abstraction, accept a backend and a store as well as nothing
    fallbacking in the last case to the ZMQ client.

    The formats accepted for the backends are:

    - redis://localhost:6379/0?name=redis-queue
    - amqp://localhost:5672?name=amqp-queue
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

        if isinstance(backend, str):
            url = urlparse(backend)
            scheme = url.scheme or 'zmq'
            assert url.scheme in {'redis', 'zmq', 'amqp', 'unix', 'tcp'}, \
                f"Unsupported {url.scheme}"
            self._backend = backends[scheme].from_url(backend, sign_data)
        elif isinstance(backend,
                        (ZMQTasqClient, RabbitMQTasqClient, RedisTasqClient)):
            self._backend = backend
        else:
            print("Unsupported backend", backend)
        # Handle only redis as a backend store for now
        if store:
            urlstore = urlparse(store)
            assert urlstore.scheme in {'redis'}, f"Unknown {scheme}"
            db = int(urlstore.path.split('/')[-1]) if url.query else 0
            self._store = RedisStore(urlstore.hostname, urlstore.port, db)
            self._results = Queue()
            Thread(target=self._store_results, daemon=True).start()
        else:
            self._store = store
        # Connect with the backend
        self._backend.connect()

    def _store_results(self):
        while True:
            tasqfuture = self._results.get()
            if isinstance(tasqfuture, TasqFuture):
                job_result = tasqfuture.result()
            else:
                job_result = tasqfuture
            self._store.put_result(job_result)

    def put(self, func, *args, **kwargs):
        tasq_result = self._backend.schedule(func, *args, **kwargs)
        if self._store:
            self._results.put(tasq_result)
        return tasq_result

    def put_blocking(self, func, *args, **kwargs):
        tasq_result = self._backend.schedule_blocking(func, *args, **kwargs)
        if self._store:
            self._results.put(tasq_result)
        return tasq_result

    def pending_jobs(self):
        return list(self._backend.pending_jobs())

    def results(self):
        return self._backend.results
