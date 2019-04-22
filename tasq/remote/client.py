"""
tasq.remote.client.py
~~~~~~~~~~~~~~~~~~~~~
Client part of the application, responsible for scheduling jobs to local or
remote workers.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from threading import Thread
from collections import deque

import zmq

from ..job import Job
from ..logger import get_logger
from ..actors.routers import RoundRobinRouter
from ..actors.actorsystem import ActorSystem
from .actors import ClientWorker
from .connection import ConnectionFactory


class TasqClientNotConnected(Exception):

    def __init__(self, msg=u''):
        self.message = msg
        super().__init__(self.message)


class BaseTasqClient(metaclass=ABCMeta):

    """
    Simple client class to schedule jobs to remote workers, currently
    supports a synchronous way of calling tasks awaiting for results and an
    asynchronous one which collect results in a dedicated dictionary
    """

    def __init__(self, host, port, sign_data=False):
        # Host address of a remote supervisor to connect to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._port = port
        # Send digital signed data
        self._sign_data = sign_data
        # Client reference, set up the communication with a Supervisor
        self._client = self._make_client()
        # Connection flag
        self._is_connected = False
        # Results dictionary, mapping task_name -> result
        self._results = {}
        # Pending requests while not connected
        self._pending = deque()
        # Gathering results, making the client unblocking
        self._gatherer = Thread(target=self._gather_results, daemon=True)
        # Logging settings
        self._log = get_logger(f'{__name__}.{self._host}.{self._port}')

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

    def __repr__(self):
        status = 'connected' if self.is_connected else 'disconnected'
        return f"<BaseTasqClient worker=(tcp://{self.host}:{self.port}, " \
               f"tcp://{self.host}:{self.port}) status={status}>"

    @abstractmethod
    def _make_client(self):
        pass

    @abstractmethod
    def _gather_results(self):
        pass

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if not self.is_connected:
            self._client.connect()
            self._is_connected = True
            # Start gathering thread
            self._gatherer.start()
            # Check if there are pending requests and in case, empty the queue
            while self._pending:
                job = self._pending.pop()
                self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected:
            self._client.disconnect()
            self._is_connected = False

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

        :rtype: concurrent.futures.Future
        :return: A future eventually containing the result of the func
                 execution
        """
        name = kwargs.pop('name', u'')
        job = Job(name, func, *args, **kwargs)
        # If not connected enqueue for execution at the first connection
        if not self.is_connected:
            self._log.debug("Client not connected, appending job to pending queue.")
            self._pending.appendleft(job)
            return None
        # Create a Future and return it, _gatherer thread will set the
        # result once received
        future = Future()
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

        :return: The result of the func execution
        """
        if not self.is_connected:
            raise TasqClientNotConnected('Client not connected to no worker')
        timeout = kwargs.pop('timeout', None)
        future = self.schedule(func, *args, **kwargs)
        result = future.result(timeout)
        return result


class ZMQTasqClient(BaseTasqClient):

    """Simple client class to schedule jobs to remote workers, currently
    supports a synchronous way of calling tasks awaiting for results and an
    asynchronous one which collect results in a dedicated dictionary
    """

    def __init__(self, host, port, plport=None, sign_data=False, unix_socket=False):
        self._plport = plport or port + 1
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        super().__init__(host, port, sign_data)

    @property
    def plport(self):
        return self._plport

    def __repr__(self):
        socket_type = 'tcp' if not self._unix_socket else 'unix'
        status = 'connected' if self.is_connected else 'disconnected'
        return f"<ZMQTasqClient worker=({socket_type}://{self.host}:{self.port}, " \
               f"{socket_type}://{self.host}:{self.plport}) status={status}>"

    def _make_client(self):
        return ConnectionFactory \
            .make_client(self.host, self.port, self.plport,
                         self._sign_data, self._unix_socket)

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while True:
            try:
                job_result = self._client.recv()
            except (zmq.error.ContextTerminated, zmq.error.ZMQError):
                self._log.warning("ZMQ error while receiving results back")
                pass
            if not job_result:
                continue
            self._log.debug("Gathered result: %s", job_result)
            try:
                if not job_result.value and job_result.exc:
                    self._results[job_result.name].set_result(job_result.exc)
                else:
                    self._results[job_result.name].set_result(job_result.value)
            except KeyError:
                self._log.error("Can't update result: key not found")


class RedisTasqClient(BaseTasqClient):

    """"""

    def __init__(self, host, port, db, name, sign_data=False):
        self._db = db
        self._name = name
        super().__init__(host, port, sign_data)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        status = 'connected' if self.is_connected else 'disconnected'
        return f"<RedisTasqClient worker=(redis://{self.host}:{self.port}, " \
               f"redis://{self.host}:{self.port}) status={status}>"

    def _make_client(self):
        return ConnectionFactory \
            .make_redis_client(self.host, self.port, self._db,
                               self._name, secure=self._sign_data)

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while True:
            job_result = self._client.recv_result()
            if not job_result:
                continue
            self._log.debug("Gathered result: %s", job_result)
            try:
                if not job_result.value and job_result.exc:
                    self._results[job_result.name].set_result(job_result.exc)
                else:
                    self._results[job_result.name].set_result(job_result.value)
            except KeyError:
                self._log.error("Can't update result: key not found")

    def pending_jobs(self):
        return self._client.get_pending_jobs()

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if not self.is_connected:
            self._is_connected = True
            # Start gathering thread
            self._gatherer.start()
            # Check if there are pending requests and in case, empty the queue
            while self._pending:
                job = self._pending.pop()
                self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected:
            self._is_connected = False


class RabbitMQTasqClient(BaseTasqClient):

    """"""

    def __init__(self, host, port, name, sign_data=False):
        self._name = name
        super().__init__(host, port, sign_data)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        status = 'connected' if self.is_connected else 'disconnected'
        return f"<RabbitMQTasqClient worker=(amqp://{self.host}:{self.port}, " \
               f"amqp://{self.host}:{self.port}) status={status}>"

    def _make_client(self):
        return ConnectionFactory \
            .make_rabbitmq_client(self.host, self.port, 'sender',
                                  self._name, secure=self._sign_data)

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently
        listen for results and store them into a dedicated dictionary
        """
        while True:
            job_result = self._client.recv_result()
            self._log.debug("Gathered result: %s", job_result)
            try:
                if not job_result.value and job_result.exc:
                    self._results[job_result.name].set_result(job_result.exc)
                else:
                    self._results[job_result.name].set_result(job_result.value)
            except KeyError:
                self._log.error("Can't update result: key not found")

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels,
        respectively used to send tasks and to retrieve results back
        """
        if not self.is_connected:
            self._is_connected = True
            # Start gathering thread
            self._gatherer.start()
            # Check if there are pending requests and in case, empty the queue
            while self._pending:
                job = self._pending.pop()
                self.schedule(job.func, *job.args, name=job.job_id, **job.kwargs)

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        if self.is_connected:
            self._is_connected = False


class TasqClientPool:

    """Basic client pool, defining methods to talk to multiple remote
    workers"""

    # TODO WIP - still a rudimentary implementation

    def __init__(self, config, router_class=RoundRobinRouter):
        # List of tuples defining host:port pairs to connect
        self._config = config
        # Router class
        self._router_class = router_class
        # Pool of clients
        self._clients = [
            ZMQTasqClient(host,
                          psport,
                          plport) for host, psport, plport in self._config
        ]
        # Collect results in a dictionary
        self._results = {}
        # Workers actor system
        self._system = ActorSystem('clientpool-actorsystem')
        # Workers pool
        self._workers = self._system.router_of(
            num_workers=len(self._clients),
            actor_class=ClientWorker,
            router_class=self._router_class,
            clients=self._clients
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
        name = kwargs.pop('name', u'')
        job = Job(name, func, *args, **kwargs)
        future = self._workers.route(job)
        self._results[job.job_id] = future
        return future

    def schedule_blocking(self, func, *args, **kwargs):
        """Schedule a job to a remote worker, awaiting for it to finish its
        execution.
        """
        timeout = kwargs.pop('timeout', None)
        future = self.schedule(func, *args, **kwargs)
        result = future.result(timeout)
        return result
