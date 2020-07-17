"""
tasq.queue.py
~~~~~~~~~~~~~
The main client module, provides interfaces to instantiate queues
"""

from queue import Queue
from threading import Thread
from urllib.parse import urlparse
from .remote.client import (
    ZMQClient,
    RedisClient,
    RabbitMQClient,
    TasqFuture,
)


backends = {
    "redis": RedisClient,
    "amqp": RabbitMQClient,
    "unix": ZMQClient,
    "zmq": ZMQClient,
    "tcp": ZMQClient,
}


class TasqQueue:

    """Main queue abstraction, accept a backend and a store as well as nothing
    fallbacking in the last case to the ZMQ client.

    Attributes
    ----------
    :type backend: str or u'zmq://localhost:9000'
    :param backend: An URL to connect to for the backend service

    :type store: str or None
    :param store: An URL to connect to for the results store service
    """

    def __init__(self, backend, store=None):
        self._backend = backend
        # Handle only redis as a backend store for now
        self._store = store
        if store:
            self._results = Queue()
            Thread(target=self._store_results, daemon=True).start()
        # Connect with the backend
        self._backend.connect()

    def __len__(self):
        return len(self.pending_jobs())

    def _store_results(self):
        while True:
            tasqfuture = self._results.get()
            if isinstance(tasqfuture, TasqFuture):
                job_result = tasqfuture.result()
            else:
                job_result = tasqfuture
            self._store.put_result(job_result)

    def connect(self):
        if not self._backend.is_connected():
            self._backend.connect()

    def disconnect(self):
        if self._backend.is_connected():
            self._backend.disconnect()

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
