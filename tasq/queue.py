"""
tasq.queue.py
~~~~~~~~~~~~~
The main client module, provides interfaces to instantiate queues
"""

from queue import Queue
from threading import Thread
from .job import Job
from .remote.client import TasqFuture


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

    def __repr__(self):
        return f"TasqQueue({self._backend})"

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

    def is_connected(self):
        return self._backend.is_connected()

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


class TasqMultiQueue:
    def __init__(self, backends, router_factory):
        # List of backend clients
        self._backends = backends
        # Router to spread jobs
        self._router = router_factory()

    def __len__(self):
        return sum(len(b.pending_jobs()) for b in self._backends)

    def is_connected(self):
        return any([backend.is_connected() for backend in self._backends])

    def connect(self):
        for backend in self._backends:
            backend.connect()

    def disconnect(self):
        for backend in self._backends:
            backend.disconnect()

    def pending_jobs(self):
        jobs = []
        for backend in self._backends:
            jobs.extend(backend.pending_jobs())
        return jobs

    def shutdown(self):
        """Close all connected clients"""
        for backend in self._backends:
            backend.disconnect()

    def map(self, func, iterable):
        """Schedule a list of jobs represented by `iterable` in a round-robin
        manner. Can be seen as equivalent as schedule with `RoundRobinRouter`
        routing.
        """
        idx = 0
        for args, kwargs in iterable:
            if idx == len(self._clients) - 1:
                idx = 0
            # Lazy check for connection
            if not self._backends[idx].is_connected():
                self._backends[idx].connect()
            self._backends[idx].schedule(func, *args, **kwargs)

    def put(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, without blocking. Require a
        func task, and arguments to be passed with, cloudpickle will handle
        dependencies shipping. Optional it is possible to give a name to the
        job, otherwise a UUID will be defined
        """
        name = kwargs.pop("name", "")
        job = Job(name, func, *args, **kwargs)
        future = self._router.route(job)
        return future

    def put_blocking(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, awaiting for it to finish its
        execution.
        """
        timeout = kwargs.pop("timeout", None)
        future = self.put(func, *args, **kwargs)
        result = future.result(timeout)
        return result

    def results(self):
        return [backend.results for backend in self._backends]
