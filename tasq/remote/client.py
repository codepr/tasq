"""
tasq.remote.client.py
~~~~~~~~~~~~~~~~~~~~~
Client part of the application, responsible for scheduling jobs to local or
remote workers.
"""

from urllib.parse import urlparse
from abc import ABC, abstractmethod
from concurrent.futures import Future
from threading import Thread, Event
from collections import deque

import zmq

import tasq.worker as worker
import tasq.actors as actors
from ..job import Job, JobStatus
from ..logger import get_logger
from .connection import (
    ZMQBackendConnection,
    connect_rabbitmq_backend,
    connect_redis_backend,
)


class ClientNotConnected(Exception):
    pass


class TasqFuture(Future):
    def unwrap(self):
        job_result = self.result()
        if job_result.outcome == JobStatus.FAILED:
            return job_result.exc
        return job_result.value

    def exec_time(self):
        job_result = self.result()
        return job_result.exec_time


class Client(ABC):

    """Simple client class to schedule jobs to remote workers, currently
    supports a synchronous way of calling tasks awaiting for results and an
    asynchronous one which collect results in a dedicated dictionary

    Attributes
    ----------
    :type host: str
    :param host: The IP address to connect with

    :type port: int
    :param port: The port associated with the host param

    :type signkey: str or None
    :param signkey: String representing a sign, marks bytes passing around
                    through sockets
    """

    def __init__(self, host, port, signkey=None):
        # Host address of a remote supervisor to connect to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._port = port
        # Send digital signed data
        self._signkey = signkey
        # Client reference, set up the communication with a Supervisor
        self._client = None
        # Connection flag
        self._is_connected = False
        # Results dictionary, mapping task_name -> result
        self._results = {}
        # Pending requests while not connected
        self._pending = deque()
        # Gathering results, making the client unblocking
        self._gatherer = None
        # Logging settings
        self._log = get_logger(f"{__name__}.{self._host}.{self._port}")

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def is_connected(self):
        return self._is_connected

    @property
    def pending(self):
        return self._pending

    @property
    def results(self):
        return self._results

    def __enter__(self):
        if not self.is_connected:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        while self.pending_results():
            pass
        self.close()

    @abstractmethod
    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        raise NotImplementedError()

    @abstractmethod
    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        raise NotImplementedError()

    def close(self):
        """Close sockets connected to workers, destroy zmq cotext"""
        if self.is_connected:
            self.disconnect()
        self._client.close()

    def pending_jobs(self):
        """Returns the pending jobs"""
        return self._pending

    def pending_results(self):
        """Retrieve pending jobs from the results dictionary"""
        return {k: v for k, v in self._results.items() if v.done() is False}

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
        if not self.is_connected:
            self._log.debug(
                "Client not connected, appending job to pending queue."
            )
            self._pending.appendleft(job)
            return None
        # Create a Future and return it, _gatherer thread will set the
        # result once received
        future = TasqFuture()
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

        :raise: tasq.remote.client.ClientNotConnected, in case of not
                connected client
        """
        if not self.is_connected:
            raise ClientNotConnected("Client not connected to no worker")
        timeout = kwargs.pop("timeout", None)
        future = self.schedule(func, *args, **kwargs)
        result = future.result(timeout)
        return result


class ZMQClient(Client):

    """Simple client class to schedule jobs to remote workers, currently
    supports a synchronous way of calling tasks awaiting for results and an
    asynchronous one which collect results in a dedicated dictionary

    Attributes
    ----------
    :type host: str
    :param host: The IP address to connect with

    :type port: int
    :param port: The port associated with the host param for PUSH channel
                 communication

    :type plport: int or None
    :param plport: The pull port to retrieve bytes from

    :type signkey: bool or False
    :param signkey: Boolean flag, sign bytes passing around through sockets
                      if True

    :type unix_socket: bool or False
    :param unix_socket: Boolean flag to decide wether to use a UNIX socket or a
                        TCP one
    """

    __extraparams__ = {"plport"}

    def __init__(
        self, host, port, plport=None, signkey=None, unix_socket=False
    ):
        self._plport = plport or port + 1
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        self._gather_loop = Event()
        super().__init__(host, port, signkey)

    @property
    def plport(self):
        return self._plport

    def __repr__(self):
        socket_type = "tcp" if not self._unix_socket else "unix"
        status = "connected" if self.is_connected else "disconnected"
        return (
            f"<ZMQClient worker=({socket_type}://{self.host}:{self.port}, "
            f"{socket_type}://{self.host}:{self.plport}) status={status}>"
        )

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while not self._gather_loop.is_set():
            try:
                job_result = self._client.recv()
            except (zmq.error.ContextTerminated, zmq.error.ZMQError):
                self._log.warning("ZMQ error while receiving results back")
            if not job_result:
                continue
            self._log.debug("Gathered result: %s", job_result)
            try:
                self._results[job_result.name].set_result(job_result)
            except KeyError:
                self._log.error("Can't update result: key not found")

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if self.is_connected:
            return
        if not self._client:
            self._client = ZMQBackendConnection(
                self.host,
                self.port,
                self.plport,
                self._unix_socket,
                self._signkey,
            )
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
        if self.is_connected:
            self._gather_loop.set()
            self._gatherer.join()
            self._client.disconnect()
            self._is_connected = False

    @classmethod
    def from_url(cls, url, signkey=None):
        u = urlparse(url)
        scheme = u.scheme or "zmq"
        assert scheme in ("zmq", "unix", "tcp"), f"Unsupported {scheme}"
        extras = {
            t.split("=")[0]: t.split("=")[1] for t in u.query.split("?") if t
        }
        extras = {k: v for k, v in extras.items() if k in cls.__extraparams__}
        conn_args = {
            "host": u.hostname or "127.0.0.1",
            "port": u.port or 9000,
            "plport": int(extras.get("plport", 0)),
            "signkey": signkey,
            "unix_socket": scheme == "unix",
        }
        return cls(**conn_args)


class RedisClient(Client):

    """Simple Redis client class to schedule jobs to remote workers using
    redis as the backend broker.

    Attributes
    ----------
    :type host: str or 'localhost'
    :param host: The IP address of the Redis instance to connect with

    :type port: int or 6379
    :param port: The port associated with the host param

    :type db: int or 0
    :param db: The database to use on redis for the queues

    :type name: str or redis-queue
    :param name: The name of the redis queue

    :type signkey: bool or False
    :param signkey: Boolean flag, sign bytes passing around through sockets
                      if True
    """

    __extraparams__ = {"db", "name"}

    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        name="redis-queue",
        signkey=None,
    ):
        self._db = db
        self._name = name
        self._gather_loop = Event()
        super().__init__(host, port, signkey)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        status = "connected" if self.is_connected else "disconnected"
        return (
            f"<RedisClient worker=(redis://{self.host}:{self.port}, "
            f"redis://{self.host}:{self.port}) status={status}>"
        )

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while not self._gather_loop.is_set():
            job_result = self._client.recv_result()
            if not job_result:
                continue
            self._log.debug("Gathered result: %s", job_result)
            try:
                self._results[job_result.name].set_result(job_result)
            except KeyError:
                self._log.error("Can't update result: key not found")

    def pending_jobs(self):
        return self._client.get_pending_jobs()

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if self.is_connected:
            return
        if not self._client:
            self._client = connect_redis_backend(
                self.host,
                self.port,
                self._db,
                self._name,
                signkey=self._signkey,
            )
        # Gathering results, making the client unblocking
        if not self._gatherer:
            self._gatherer = Thread(target=self._gather_results, daemon=True)
            # Start gathering thread
            self._gatherer.start()
        elif not self._gatherer.is_alive():
            self._gather_loop.clear()
            self._gatherer.start()
        self._is_connected = True
        # Check if there are pending requests and in case, empty the queue
        while self._pending:
            job = self._pending.pop()
            self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected:
            self._gather_loop.set()
            self._gatherer.join()
            self._is_connected = False

    @classmethod
    def from_url(cls, url, signkey=None):
        u = urlparse(url)
        scheme = u.scheme or "redis"
        assert scheme == "redis", f"Unsupported {scheme}"
        extras = {
            t.split("=")[0]: t.split("=")[1] for t in u.query.split("?") if t
        }
        extras = {k: v for k, v in extras.items() if k in cls.__extraparams__}
        conn_args = {
            "host": u.hostname or "localhost",
            "port": u.port or 6379,
            "db": int(extras.get("db", 0)),
            "name": extras.get("name", "redis-queue"),
            "signkey": signkey,
        }
        return cls(**conn_args)


class RabbitMQClient(Client):

    """Simple RabbitMQ client class to schedule jobs to remote workers using
    RabbitMQ as the backend broker.

    Attributes
    ----------
    :type host: str or 'localhost'
    :param host: The IP address of the RabbitMQ instance to connect with

    :type port: int or 5672
    :param port: The port associated with the host param

    :type name: str or amqp-queue
    :param name: The name of the RabbitMQ queue

    :type signkey: bool or False
    :param signkey: Boolean flag, sign bytes passing around through sockets
                    if True
    """

    __extraparams__ = {"name"}

    def __init__(
        self, host="localhost", port=5672, name="amqp-queue", signkey=None
    ):
        self._name = name
        self._gatherer_loop = Event()
        super().__init__(host, port, signkey)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        status = "connected" if self.is_connected else "disconnected"
        return (
            f"<RabbitMQClient worker=(amqp://{self.host}:{self.port}) "
            f"status={status}>"
        )

    def _make_client(self):
        return connect_rabbitmq_backend(
            self.host, self.port, "sender", self._name, signkey=self._signkey
        )

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while not self._gatherer_loop.is_set():
            job_result = self._client.recv_result()
            if not job_result:
                continue
            self._log.debug("Gathered result: %s", job_result)
            try:
                self._results[job_result.name].set_result(job_result)
            except KeyError:
                self._log.error("Can't update result: key not found")

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if self.is_connected:
            return
        if not self._client:
            self._client = connect_rabbitmq_backend(
                self.host,
                self.port,
                "sender",
                self._name,
                signkey=self._signkey,
            )
        # Gathering results, making the client unblocking
        if not self._gatherer:
            self._gatherer = Thread(target=self._gather_results, daemon=True)
            # Start gathering thread
            self._gatherer.start()
        elif not self._gatherer.is_alive():
            self._gather_loop.clear()
            # Gathering results, making the client unblocking
            self._gatherer.start()
        self._is_connected = True
        # Check if there are pending requests and in case, empty the queue
        while self._pending:
            job = self._pending.pop()
            self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected:
            self._gather_loop.set()
            self._gatherer.join()
            self._is_connected = False

    @classmethod
    def from_url(cls, url, signkey=None):
        u = urlparse(url)
        scheme = u.scheme or "amqp"
        assert scheme == "amqp", f"Unsupported {scheme}"
        extras = {
            t.split("=")[0]: t.split("=")[1] for t in u.query.split("?") if t
        }
        extras = {k: v for k, v in extras.items() if k in cls.__extraparams__}
        conn_args = {
            "host": u.hostname or "localhost",
            "port": u.port or 5672,
            "name": extras.get("name", "amqp-queue"),
            "signkey": signkey,
        }
        return cls(**conn_args)


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
            ZMQClient(host, psport, plport)
            for host, psport, plport in self._config
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
            if not self._clients[idx].is_connected:
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
