"""
tasq.remote.client.py
~~~~~~~~~~~~~~~~~~~~~
Client part of the application, responsible for scheduling jobs to local or
remote workers.
"""

from concurrent.futures import Future
from threading import Thread, Event
from collections import deque
import tasq.worker as worker
import tasq.actors as actors
from ..job import Job, JobStatus
from ..logger import get_logger
from ..exception import (
    BackendCommunicationErrorException,
    ClientNotConnectedException,
)


class TasqFuture(Future):
    def unwrap(self):
        job_result = self.result()
        if job_result.outcome == JobStatus.FAILED:
            return job_result.exc
        return job_result.value

    def exec_time(self):
        job_result = self.result()
        return job_result.exec_time


class Client:

    """Simple client class to schedule jobs to remote workers, currently
    supports a synchronous way of calling tasks awaiting for results and an
    asynchronous one which collect results in a dedicated dictionary

    Attributes
    ----------
    :type client: tasq.remote.client.Client
    :param client: The Client reference needed to communicate with remote
                   runners, can be either a `ZMQBackendConnection` or a
                   generic `BackendConnection` for backends other than a ZMQ
                   socket.

    :type signkey: str or None
    :param signkey: String representing a sign, marks bytes passing around
                    through sockets
    """

    def __init__(self, client):
        # Client backend dependency, can be a ZMQBackendConnection or a generic
        # BackendConnection for backends other than ZMQ
        self._client = client
        # Connection flag
        self._is_connected = False
        # Results dictionary, mapping task_name -> result
        self._results = {}
        # Pending requests while not connected
        self._pending = deque()
        # Gathering results, making the client unblocking
        self._gatherer = None
        # threading.Event to run and control the gatherer loop
        self._gather_loop = Event()
        # Logging settings
        self._log = get_logger(__name__)

    def __repr__(self):
        return f"Client({self._client})"

    def __enter__(self):
        if not self.is_connected():
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        while self.pending_results():
            pass
        self.close()

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while not self._gather_loop.is_set():
            try:
                job_result = self._client.recv_result()
            except BackendCommunicationErrorException:
                self._log.warning("Backend error while receiving results back")
            else:
                if not job_result:
                    continue
                self._log.debug("Gathered result: %s", job_result)
                try:
                    self._results[job_result.name].set_result(job_result)
                except KeyError:
                    self._log.error("Can't update result: key not found")

    def is_connected(self):
        return self._is_connected

    def pending_jobs(self):
        """Returns the pending jobs"""
        return self._pending

    def pending_results(self):
        """Retrieve pending jobs from the results dictionary"""
        return {k: v for k, v in self._results.items() if v.done() is False}

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if self.is_connected():
            return
        # Gathering results, making the client unblocking
        if not self._gatherer:
            self._gatherer = Thread(target=self._gather_results, daemon=True)
            # Start gathering thread
            self._gatherer.start()
        elif not self._gatherer.is_alive():
            self._gather_loop.clear()
            # Start gathering thread
            self._gatherer.start()
        self._client.connect()
        self._is_connected = True
        # Check if there are pending requests and in case, empty the queue
        while self._pending:
            job = self._pending.pop()
            self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected():
            self._gather_loop.set()
            self._gatherer.join()
            self._client.disconnect()
            self._is_connected = False

    def schedule(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, without blocking. Require a
        func task, and arguments to be passed with, cloudpickle will handle
        dependencies shipping. Optional it is possible to give a name to the
        job, otherwise a UUID will be defined

        Args:
        -----
        :type func: func
        :param func: A function to be executed on a worker by enqueing it

        :rtype: tasq.remote.client.TasqFuture
        :return: A future eventually containing the result of the func
                 execution
        """
        name = kwargs.pop("name", "")
        job = Job(name, func, *args, **kwargs)
        # If not connected enqueue for execution at the first connection
        if not self.is_connected():
            self._log.debug(
                "Client not connected, appending job to pending queue."
            )
            self._pending.appendleft(job)
            return None
        # Create a Future and return it, _gatherer thread will set the
        # result once received
        future = TasqFuture()
        if name in self._results:
            self._results.pop(name)
        self._results[name] = future
        # Send job to worker
        self._client.send(job)
        return future

    def schedule_blocking(self, func, *args, **kwargs):
        """Schedule a job to a remote worker wating for the result to be ready.
        Like `schedule` it require a func task, and arguments to be passed
        with, cloudpickle will handle dependencies shipping. Optional it is
        possible to give a name to the job, otherwise a UUID will be defined

        Args:
        -----
        :type func: func
        :param func: A function to be executed on a worker by enqueing it

        :rtype: tasq.remote.client.TasqFuture
        :return: The result of the func execution

        :raise: tasq.exception.ClientNotConnectedException, in case of not
                connected client
        """
        if not self.is_connected():
            raise ClientNotConnectedException(
                "Client not connected to no worker"
            )
        timeout = kwargs.pop("timeout", None)
        future = self.schedule(func, *args, **kwargs)
        result = future.result(timeout)
        return result


class ClientPool:

    """Basic client pool, defining methods to talk to multiple remote
    workers
    """

    # TODO WIP - still a rudimentary implementation

    def __init__(self, config, router_class=actors.RoundRobinRouter):
        # List of tuples defining host:port pairs to connect
        self._config = config
        # Router class
        self._router_class = router_class
        # Pool of clients
        self._clients = [
            ZMQClient(host, psport, pull_port)
            for host, psport, pull_port in self._config
        ]
        # Collect results in a dictionary
        self._results = {}
        # Workers actor system
        self._system = actors.get_actorsystem("clientpool-actorsystem")
        # Workers pool
        self._workers = self._system.router_of(
            num_workers=len(self._clients),
            actor_class=worker.ClientWorker,
            router_class=self._router_class,
            clients=self._clients,
        )

    @property
    def router_class(self):
        return self._router_class

    @property
    def results(self):
        """Lazily check for new results and add them to the list of
        dictionaries before returning it
        """
        return self._results

    def __iter__(self):
        return self._clients.__iter__()

    def shutdown(self):
        """Close all connected clients"""
        for client in self._clients:
            client.close()
        self._system.shutdown()

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
            if not self._clients[idx].is_connected():
                self._clients[idx].connect()
            self._clients[idx].schedule(func, *args, **kwargs)

    def schedule(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, without blocking. Require a
        func task, and arguments to be passed with, cloudpickle will handle
        dependencies shipping. Optional it is possible to give a name to the
        job, otherwise a UUID will be defined
        """
        name = kwargs.pop("name", "")
        job = Job(name, func, *args, **kwargs)
        future = self._workers.route(job)
        self._results[job.job_id] = future
        return future

    def schedule_blocking(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, awaiting for it to finish its
        execution.
        """
        timeout = kwargs.pop("timeout", None)
        future = self.schedule(func, *args, **kwargs)
        result = future.result(timeout)
        return result
