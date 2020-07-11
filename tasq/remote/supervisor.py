"""
tasq.remote.supervisor.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Supervisor process, listening for incoming connections to schedule tasks to a
pool of worker actors
"""
import os
import asyncio
from threading import Thread
from abc import ABC, ABCMeta, abstractmethod
from multiprocessing import Process, cpu_count

from .actors import ResponseActor, WorkerActor
from .connection import ConnectionFactory
from .server import ZMQTcpServer, ZMQUnixServer
from ..logger import get_logger
from ..jobqueue import JobQueue
from ..worker import ProcessQueueWorker
from ..actors.actorsystem import ActorSystem
from ..actors.routers import RoundRobinRouter, SmallestMailboxRouter


def max_workers():
    return (cpu_count() * 2) + 1


class Supervisor(ABC):

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

    :type signkey: bool or False
    :param signkey: A boolean controlling wether the serialized jobs should
                      be salted and signed or just plain bytearrays.

    """

    def __init__(self, host, port=9000, num_workers=max_workers(), signkey=None):
        # Host address to bind sockets to
        self._host = host
        self._port = port
        # Number of workers
        self._num_workers = num_workers
        # Send digital signed data
        self._signkey = signkey
        self._log = get_logger(f"{__name__}-{os.getpid()}")

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def start(self):
        pass


class ZMQSupervisor(Supervisor, metaclass=ABCMeta):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker actors, responses are sent
    back to clients by using a pool of actors as well
    """

    def __init__(
        self,
        host,
        pull_port,
        push_port,
        num_workers=max_workers(),
        signkey=None,
        unix_socket=False,
    ):
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        super().__init__(host, num_workers=num_workers, signkey=signkey)
        # Event loop
        self._loop = asyncio.get_event_loop()
        self._server = ZMQTcpServer(host, push_port, pull_port, signkey)

    def _bind_sockets(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor
        """
        self._server.bind()
        self._log.info("Listening for jobs on %s:%s", self._host, self._pull_port)

    async def run(self, unpickle=True):
        self._log.info("Running")
        while True:
            if await self._server.poll():
                self._log.info("New job")
                job = await self._server.recv(unpickle=unpickle)
                f = self._workers.route(job)
                await self._server.send(await f)

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

    def start(self):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._bind_sockets()
        asyncio.run(self.run())


class ZMQActorSupervisor(ZMQSupervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker actors, responses are sent
    back to clients by using a pool of actors as well
    """

    def __init__(
        self,
        host,
        pull_port,
        push_port,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
        unix_socket=False,
    ):
        super().__init__(host, pull_port, push_port, num_workers, signkey, unix_socket)
        # Routing type
        self._router_class = router_class
        # Worker's ActorSystem
        self._system = ActorSystem(
            f"{self._host}:({self._push_port}, {self._pull_port})"
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=None,
        )
        self._log.info("Worker type: Actor")

    @classmethod
    def create(
        cls,
        host,
        pull_port,
        push_port,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
        unix_socket=False,
    ):
        return cls(
            host, pull_port, push_port, num_workers, router_class, signkey, unix_socket
        )


class ZMQQueueSupervisor(ZMQSupervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(
        self,
        host,
        pull_port,
        push_port,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
        unix_socket=False,
    ):
        super().__init__(host, pull_port, push_port, num_workers, signkey, unix_socket)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers, worker_class=worker_class)
        # Dedicated thread to client responses
        # self._response_thread = Thread(target=self._respond, daemon=True)
        # self._response_thread.start()
        self._log.info("Worker type: %s", worker_class.__name__)

    # def _respond(self):
    #     """Spin a loop and respond to client with whatever results arrive in
    #     the completed_jobs queue
    #     """
    #     while True:
    #         response = self._jobqueue.get_result()
    #         # Poison pill check
    #         if response is None:
    #             break
    #         self._server.send(response)
    #
    # async def _start(self):
    #     """Receive jobs from clients with polling"""
    #     unpickle = False
    #     while True:
    #         if await self._server.poll():
    #             pickled_job = await self._server.recv(unpickle=unpickle)
    #             self._jobqueue.add_job(pickled_job)

    def stop(self):
        """Stops the running response threads and the pool of processes"""
        super().stop()
        # Use a poison pill to stop the loop
        self._jobqueue.shutdown()
        self._response_thread.join()

    def start(self, unpickle=True):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._bind_sockets()
        asyncio.run(self.run(False))

    @classmethod
    def create(
        cls,
        host,
        pull_port,
        push_port,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
        unix_socket=False,
    ):
        return cls(
            host, pull_port, push_port, num_workers, worker_class, signkey, unix_socket
        )


class RedisQueueSupervisor(Supervisor):

    """Supervisor process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(
        self,
        host,
        port,
        db,
        name,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
    ):
        self._db = db
        self._name = name
        super().__init__(host, port, num_workers, signkey)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers, worker_class=worker_class)
        # Dedicated thread to client responses
        self._response_thread = Thread(target=self._respond, daemon=True)
        self._response_thread.start()
        self._run = True
        self._log.info("Worker type: %s", worker_class.__name__)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_redis_client(
            self._host, self._port, self._db, self._name, signkey=self._signkey
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        # Use a poison pill to stop the loop
        self._jobqueue.shutdown()
        self._response_thread.join()

    def start(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            self._jobqueue.add_job(job)

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

    @classmethod
    def create(
        cls,
        host,
        port,
        db,
        name,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
    ):
        return cls(host, port, db, name, num_workers, worker_class, signkey)


class RedisActorSupervisor(Supervisor):
    def __init__(
        self,
        host,
        port,
        db,
        name,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
    ):
        self._db = db
        self._name = name
        super().__init__(host, port, num_workers, signkey)
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
            func_name="send",
            sendfunc=self._server.send_result,
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=self._responses,
        )
        self._log.info("Worker type: Actor")

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_redis_client(
            self._host, self._port, self._db, self._name, signkey=self._signkey
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False

    def start(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            res = self._workers.route(job)
            self._responses.route(res)
            self._log.info("Routed")

    @classmethod
    def create(
        cls,
        host,
        port,
        db,
        name,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
    ):
        return cls(host, port, db, name, num_workers, router_class, signkey)


class RabbitMQQueueSupervisor(Supervisor):

    """Supervisor process, handle requests from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(
        self,
        host,
        port,
        name,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
    ):
        self._name = name
        super().__init__(host, port, num_workers, signkey)
        # Workers class type
        self._worker_class = worker_class
        # Job queue passed in to workers
        self._jobqueue = JobQueue(num_workers=num_workers, worker_class=worker_class)
        # Dedicated thread to client responses
        self._response_thread = Thread(target=self._respond, daemon=True)
        self._response_thread.start()
        self._run = True
        self._log.info("Worker type: %s", worker_class.__name__)

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_rabbitmq_client(
            self._host, self._port, "receiver", self._name, signkey=self._signkey
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._jobqueue.shutdown()
        self._response_thread.join()

    def start(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            self._jobqueue.add_job(job)

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

    @classmethod
    def create(
        cls,
        host,
        port,
        name,
        num_workers=max_workers(),
        worker_class=ProcessQueueWorker,
        signkey=None,
    ):
        return cls(host, port, name, num_workers, worker_class, signkey)


class RabbitMQActorSupervisor(Supervisor):
    def __init__(
        self,
        host,
        port,
        name,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
    ):
        self._name = name
        super().__init__(host, port, num_workers, signkey)
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
            func_name="send",
            sendfunc=self._server.send_result,
        )
        # Generic worker actor router
        self._workers = self._system.router_of(
            num_workers=self._num_workers,
            actor_class=WorkerActor,
            router_class=self._router_class,
            response_actor=self._responses,
        )
        self._log.info("Worker type: Actor")

    @property
    def name(self):
        return self._name

    def _init_server(self):
        return ConnectionFactory.make_rabbitmq_client(
            self._host, self._port, "receiver", self._name, signkey=self._signkey
        )

    def stop(self):
        self._log.info("\nStopping..")
        self._run = False
        self._server.close()

    def start(self):
        """Receive jobs from clients with polling"""
        while self._run:
            job = self._server.recv(5)
            if not job:
                continue
            self._log.info("Received job")
            res = self._workers.route(job)
            self._responses.route(res)
            self._log.info("Routed")

    @classmethod
    def create(
        cls,
        host,
        port,
        name,
        num_workers=max_workers(),
        router_class=RoundRobinRouter,
        signkey=None,
    ):
        return cls(host, port, name, num_workers, router_class, signkey)


class Supervisors:

    """Class to handle a pool of supervisors on the same node"""

    def __init__(self, binds, signkey=None, unix_socket=False):
        # List of tuples (host, pport, plport) to bind to
        self._binds = binds
        # Digital sign data before send an receive it
        self._signkey = signkey
        # Unix socket flag, if set to true, unix sockets for interprocess
        # communication will be used and ports will be used to differentiate
        # push and pull channel
        self._unix_socket = unix_socket
        # Processes, equals the len of `binds`
        self._procs = []
        self._init_binds()

    def _serve_supervisor(self, host, psh_port, pl_port):
        m = ZMQActorSupervisor(
            host,
            psh_port,
            pl_port,
            signkey=self._signkey,
            unix_socket=self._unix_socket,
        )
        m.start()

    def _init_binds(self):
        self._procs = [
            Process(target=self._serve_supervisor, args=(host, psh_port, pl_port,))
            for host, psh_port, pl_port in self._binds
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


class SupervisorFactory:
    def __init__(self):
        self._builders = {}

    def register_builder(self, key, builder):
        self._builders[key] = builder

    def create(self, key, **kwargs):
        builder = self._builders.get(key)
        if not builder:
            raise ValueError(key)
        return builder(**kwargs)


supervisor_factory = SupervisorFactory()
supervisor_factory.register_builder("ZMQ_ACTOR_SUPERVISOR", ZMQActorSupervisor.create)
supervisor_factory.register_builder("ZMQ_QUEUE_SUPERVISOR", ZMQQueueSupervisor.create)
supervisor_factory.register_builder(
    "REDIS_QUEUE_SUPERVISOR", RedisQueueSupervisor.create
)
supervisor_factory.register_builder(
    "REDIS_ACTOR_SUPERVISOR", RedisActorSupervisor.create
)
supervisor_factory.register_builder(
    "AMQP_QUEUE_SUPERVISOR", RabbitMQQueueSupervisor.create
)
supervisor_factory.register_builder(
    "AMQP_ACTOR_SUPERVISOR", RabbitMQActorSupervisor.create
)

