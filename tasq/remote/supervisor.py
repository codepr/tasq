"""
tasq.remote.supervisor.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Supervisor process, listening for incoming connections to schedule tasks to a
pool of worker actors
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import signal
import asyncio
from threading import Thread, Event
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, cpu_count

from .actors import ResponseActor, WorkerActor
from .connection import ConnectionFactory
from ..logger import get_logger
from ..jobqueue import JobQueue
from ..worker import ProcessQueueWorker
from ..actors.actorsystem import ActorSystem
from ..actors.routers import RoundRobinRouter, SmallestMailboxRouter


def max_workers():
    return (cpu_count() * 2) + 1


class BaseSupervisor(metaclass=ABCMeta):

    """Interface for a generic Supervisor, define the basic traits of
    Supervisor process, a class in charge of intermediation between clients
    and workers, being them ThreadQueue, ProcessQueue or Actors.

    Attributes
    ----------
    :type host: str
    :param host: The address to bind a listening socket to

    :type port: int or 9000
    :param port: The port associated to the address to listen for incoming
                 client connections

    :type num_workers: int or max_workers()
    :param num_workers: The number of workers to spawn on the node (e.g.
                        machine where the Supervisor is started), fallback to
                        a maximum defined by the (nr. of core x 2) + 1

    :type sign_data: bool or False
    :param sign_data: A boolean controlling wether the serialized jobs should
                      be salted and signed or just plain bytearrays.

    """

    def __init__(self, host, port=9000,
                 num_workers=max_workers(), sign_data=False):
        # Host address to bind sockets to
        self._host = host
        self._port = port
        # Number of workers
        self._num_workers = num_workers
        # Send digital signed data
        self._sign_data = sign_data
        self._log = get_logger(f'{__name__}-{os.getpid()}')
        self._server = self._init_server()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def num_workers(self):
        return self._num_workers

    @property
    def sign_data(self):
        return self._sign_data

    @abstractmethod
    def _init_server(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def serve_forever(self):
        pass


class ZMQSupervisor(BaseSupervisor, metaclass=ABCMeta):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker actors, responses are sent
    back to clients by using a pool of actors as well
    """

    def __init__(self, host, pull_port, push_port, num_workers=max_workers(),
                 sign_data=False, unix_socket=False):
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        super().__init__(host, num_workers=num_workers, sign_data=sign_data)
        # Event loop
        self._loop = asyncio.get_event_loop()
        # Handling loop exit
        self._loop.add_signal_handler(signal.SIGINT, self.stop)

    @property
    def push_port(self):
        return self._push_port

    @property
    def pull_port(self):
        return self._pull_port

    @property
    def unix_socket(self):
        return self._unix_socket

    def _init_server(self):
        """Init the server placeholder"""
        return ConnectionFactory.make_server(
            self.host, self.push_port, self.pull_port,
            self.sign_data, self.unix_socket
        )

    def _bind_sockets(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor
        """
        self._server.bind()
        self._log.info("Listening for jobs on %s:%s",
                       self._host, self._pull_port)

    @abstractmethod
    async def _start(self):
        pass

    def stop(self):
        """Stops the loop after canceling all remaining tasks"""
        self._log.info("\nStopping..")
        # Cancel pending tasks (opt)
        for task in asyncio.Task.all_tasks():
            task.cancel()
        # Stop the running loop
        self._loop.stop()
        # Stop server connection
        self._server.stop()

    def serve_forever(self):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._bind_sockets()
        asyncio.ensure_future(self._start())
        self._loop.run_forever()


class ZMQActorSupervisor(ZMQSupervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker actors, responses are sent
    back to clients by using a pool of actors as well
    """

    def __init__(self, host, pull_port, push_port, num_workers=max_workers(),
                 router_class=RoundRobinRouter, sign_data=False, unix_socket=False):
        super().__init__(host, pull_port, push_port, num_workers, sign_data, unix_socket)
        # Routing type
        self._router_class = router_class
        # Worker's ActorSystem
        self._system = ActorSystem(
            f'{self._host}:({self._push_port}, {self._pull_port})'
        )
        # Actor router for responses
        self._responses = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=ResponseActor,
            router_class=SmallestMailboxRouter,
            func_name='send',
            sendfunc=self._server.send
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=self._responses
        )
        self._log.info("Worker type: Actor")

    async def _start(self):
        """Receive jobs from clients with polling"""
        while True:
            if await self._server.poll():
                job = await self._server.recv()
                res = self._workers.route(job)
                self._responses.route(res)


class ZMQQueueSupervisor(ZMQSupervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(self, host, pull_port, push_port, num_workers=max_workers(),
                 worker_class=ProcessQueueWorker, sign_data=False, unix_socket=False):
        super().__init__(host, pull_port, push_port, num_workers, sign_data, unix_socket)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers,
                                  worker_class=worker_class)
        # Dedicated thread to client responses
        self._response_thread = Thread(target=self._respond, daemon=True)
        self._response_thread.start()
        self._log.info("Worker type: %s", worker_class.__name__)

    def _respond(self):
        """Spin a loop and respond to client with whatever results arrive in
        the completed_jobs queue
        """
        while True:
            response = self._jobqueue.get_result()
            # Poison pill check
            if response is None:
                break
            self._server.send(response)

    async def _start(self):
        """Receive jobs from clients with polling"""
        unpickle = False
        while True:
            if await self._server.poll():
                pickled_job = await self._server.recv(unpickle=unpickle)
                self._jobqueue.add_job(pickled_job)

    def stop(self):
        """Stops the running response threads and the pool of processes"""
        super().stop()
        # Use a poison pill to stop the loop
        self._jobqueue.shutdown()
        self._response_thread.join()

    def serve_forever(self):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._bind_sockets()
        asyncio.ensure_future(self._start())
        self._loop.run_forever()


class RedisQueueSupervisor(BaseSupervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(self, host, port, db, name, num_workers=max_workers(),
                 worker_class=ProcessQueueWorker, sign_data=False):
        self._db = db
        self._name = name
        super().__init__(host, port, num_workers, sign_data)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers,
                                  worker_class=worker_class)
        # Dedicated thread to client responses
        self._response_thread = Thread(target=self._respond, daemon=True)
        self._response_thread.start()
        self._run = True
        self._done = Event()
        self._log.info("Worker type: %s", worker_class.__name__)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_redis_client(
            self._host, self._port, self._db,
            self._name, secure=self._sign_data
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._done.wait()
        # Use a poison pill to stop the loop
        self._jobqueue.shutdown()
        self._response_thread.join()

    def serve_forever(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            self._jobqueue.add_job(job)
        self._done.set()

    def _respond(self):
        """Spin a loop and respond to client with whatever results arrive in
        the completed_jobs queue
        """
        while True:
            response = self._jobqueue.get_result()
            # Poison pill check
            if response is None:
                break
            self._server.send(response)


class RedisActorSupervisor(BaseSupervisor):

    def __init__(self, host, port, db, name, num_workers=max_workers(),
                 router_class=RoundRobinRouter, sign_data=False):
        self._db = db
        self._name = name
        super().__init__(host, port, num_workers, sign_data)
        # Routing type
        self._router_class = router_class
        # Worker's ActorSystem
        self._system = ActorSystem()
        self._run = True
        # Actor router for responses
        self._responses = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=ResponseActor,
            router_class=SmallestMailboxRouter,
            func_name='send',
            sendfunc=self._server.send_result
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=self._responses
        )
        self._log.info("Worker type: Actor")
        self._done = Event()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_redis_client(
            self._host, self._port, self._db,
            self._name, secure=self._sign_data
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._done.wait()

    def serve_forever(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            res = self._workers.route(job)
            self._responses.route(res)
            self._log.info("Routed")
        self._done.set()


class RabbitMQQueueSupervisor(BaseSupervisor):

    """Supervisor process, handle requests from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(self, host, port, name, num_workers=max_workers(),
                 worker_class=ProcessQueueWorker, sign_data=False):
        self._name = name
        super().__init__(host, port, num_workers, sign_data)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers,
                                  worker_class=worker_class)
        # Dedicated thread to client responses
        self._response_thread = Thread(target=self._respond, daemon=True)
        self._response_thread.start()
        self._run = True
        self._done = Event()
        self._log.info("Worker type: %s", worker_class.__name__)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_rabbitmq_client(
            self._host, self._port, 'receiver',
            self._name, secure=self._sign_data
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._done.wait()
        self._jobqueue.shutdown()
        self._response_thread.join()

    def serve_forever(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            self._jobqueue.add_job(job)
        self._done.set()

    def _respond(self):
        """Spin a loop and respond to client with whatever results arrive in
        the completed_jobs queue
        """
        while True:
            response = self._jobqueue.get_result()
            # Poison pill check
            if response is None:
                break
            self._server.send(response)


class RabbitMQActorSupervisor(BaseSupervisor):

    def __init__(self, host, port, name, num_workers=max_workers(),
                 router_class=RoundRobinRouter, sign_data=False):
        self._name = name
        super().__init__(host, port, num_workers, sign_data)
        # Routing type
        self._router_class = router_class
        # Worker's ActorSystem
        self._system = ActorSystem()
        self._run = True
        # Actor router for responses
        self._responses = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=ResponseActor,
            router_class=SmallestMailboxRouter,
            func_name='send',
            sendfunc=self._server.send_result
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=self._responses
        )
        self._log.info("Worker type: Actor")
        self._done = Event()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_rabbitmq_client(
            self._host, self._port, 'receiver',
            self._name, secure=self._sign_data
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._server.close()
        self._done.wait()

    def serve_forever(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            res = self._workers.route(job)
            self._responses.route(res)
            self._log.info("Routed")
        self._done.set()


class Supervisors:

    """Class to handle a pool of supervisors on the same node"""

    def __init__(self, binds, sign_data=False, unix_socket=False):
        # List of tuples (host, pport, plport) to bind to
        self._binds = binds
        # Digital sign data before send an receive it
        self._sign_data = sign_data
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        # Processes, equals the len of `binds`
        self._procs = []
        self._init_binds()

    def _serve_supervisor(self, host, psh_port, pl_port):
        m = ZMQActorSupervisor(host, psh_port, pl_port, sign_data=self._sign_data,
                               unix_socket=self._unix_socket)
        m.serve_forever()

    def _init_binds(self):
        self._procs = [
            Process(
                target=self._serve_supervisor,
                args=(host, psh_port, pl_port,)
            ) for host, psh_port, pl_port in self._binds
        ]

    def start_procs(self):
        for proc in self._procs:
            proc.start()
        try:
            for proc in self._procs:
                proc.join()
        except KeyboardInterrupt:
            # Clean up should be placed
            pass
