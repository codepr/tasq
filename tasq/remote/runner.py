"""
tasq.remote.runner.py
~~~~~~~~~~~~~~~~~~~~~
Runner process, listening for incoming connections to schedule tasks to a pool
of worker actors
"""
import os
import asyncio
from multiprocessing import Process, cpu_count

import tasq.worker as worker
import tasq.actors as actors
from .backend import ZMQBackend
from .connection import connect_redis_backend, connect_rabbitmq_backend
from ..logger import get_logger


def max_workers():
    return (cpu_count() * 2) + 1


class Runner:
    def __init__(self, backend, worker_factory, unpickle=True, signkey=None):
        # Send digital signed data
        self._signkey = signkey
        self._backend = backend
        self._workers = worker_factory()
        self._run = False
        self._unpickle = unpickle
        self._log = get_logger(f"{__name__}-{os.getpid()}")

    def _respond(self, fut):
        self._backend.send_result(fut.result())

    def stop(self):
        """Stops the loop after canceling all remaining tasks"""
        self._log.info("Stopping..")
        # Stop server connection
        self._run = False
        self._backend.stop()

    def start(self):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._run = True
        self.run()

    def run(self):
        while self._run:
            job = self._backend.recv(5, unpickle=self._unpickle)
            if not job:
                continue
            fut = self._workers.route(job)
            fut.add_done_callback(self._respond)


class ZMQRunner:
    """Runner process, handle requests asynchronously from clients and
    delegate processing of incoming tasks to worker processes, responses are
    sent back to clients by using a dedicated thread
    """

    def __init__(self, backend, worker_factory, unpickle=True, signkey=None):
        # Send digital signed data
        self._signkey = signkey
        self._backend = backend
        self._workers = worker_factory()
        self._unpickle = unpickle
        self._run = False
        self._log = get_logger(f"{__name__}-{os.getpid()}")
        self._loop = asyncio.get_event_loop()

    def stop(self):
        """Stops the loop after canceling all remaining tasks"""
        self._log.info("Stopping..")
        self._run = False
        # Cancel pending tasks (opt)
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self._loop.stop()
        self._loop.close()
        # Stop server connection
        self._backend.stop()

    def start(self):
        """Blocking function, schedule the execution of the coroutine waiting
        for incoming tasks and run the asyncio loop forever
        """
        self._backend.bind()
        self._run = True
        self._loop.create_task(self.run())
        self._loop.run_forever()

    async def run(self):
        while self._run:
            try:
                if await self._backend.poll():
                    job = await self._backend.recv(unpickle=self._unpickle)
                    f = self._workers.route(job)
                    fut = asyncio.wrap_future(f)
                    await self._backend.send(await fut)
            except asyncio.CancelledError:
                pass


class Runners:

    """Class to handle a pool of runners on the same node"""

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

    def _serve_runner(self, host, psh_port, pl_port):
        m = ZMQActorRunner(
            host,
            psh_port,
            pl_port,
            signkey=self._signkey,
            unix_socket=self._unix_socket,
        )
        m.start()

    def _init_binds(self):
        self._procs = [
            Process(target=self._serve_runner, args=(host, psh_port, pl_port,))
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


class RunnerFactory:
    def __init__(self):
        self._builders = {}

    def register_builder(self, key, builder):
        self._builders[key] = builder

    def create(self, key, **kwargs):
        builder = self._builders.get(key)
        if not builder:
            raise ValueError(key)
        return builder(**kwargs)


def build_zmq_actor_runner(
    host,
    channel,
    router_class=actors.RoundRobinRouter,
    num_workers=max_workers(),
    signkey=None,
    unix=False,
    unpickle=True,
):
    push, pull = channel
    ctx = actors.get_actorsystem(f"{host}:({push}, {pull})")
    server = ZMQBackend(host, push, pull, signkey, unix)
    return ZMQRunner(
        server,
        lambda: worker.build_worker_actor_router(
            router_class, num_workers, ctx
        ),
        unpickle,
        signkey,
    )


def build_zmq_queue_runner(
    host,
    channel,
    num_workers=max_workers(),
    signkey=None,
    unix=False,
    unpickle=False,
):
    push, pull = channel
    server = ZMQBackend(host, push, pull, signkey, unix)
    return ZMQRunner(
        server, lambda: worker.build_jobqueue(num_workers), unpickle, signkey
    )


def build_redis_actor_runner(
    host,
    port,
    db,
    name,
    namespace="queue",
    num_workers=max_workers(),
    router_class=actors.RoundRobinRouter,
    signkey=None,
):
    ctx = actors.get_actorsystem("")
    server = connect_redis_backend(
        host, port, db, name, namespace, signkey=signkey
    )
    return Runner(
        server,
        lambda: worker.build_worker_actor_router(
            router_class, num_workers, ctx
        ),
        signkey=signkey,
    )


def build_redis_queue_runner(
    host,
    port,
    db,
    name,
    namespace="queue",
    num_workers=max_workers(),
    signkey=None,
):
    server = connect_redis_backend(
        host, port, db, name, namespace, signkey=signkey
    )
    return Runner(
        server, lambda: worker.build_jobqueue(num_workers), False, signkey
    )


def build_rabbitmq_actor_runner(
    host,
    port,
    role,
    name,
    namespace="queue",
    num_workers=max_workers(),
    router_class=actors.RoundRobinRouter,
    signkey=None,
):
    ctx = actors.get_actorsystem("")
    server = connect_rabbitmq_backend(
        host, port, role, name, namespace, signkey=signkey
    )
    return Runner(
        server,
        lambda: worker.build_worker_actor_router(
            router_class, num_workers, ctx
        ),
        signkey=signkey,
    )


def build_rabbitmq_queue_runner(
    host,
    port,
    role,
    name,
    namespace="queue",
    num_workers=max_workers(),
    signkey=None,
):
    server = connect_rabbitmq_backend(
        host, port, role, name, namespace, signkey=signkey
    )
    return Runner(
        server, lambda: worker.build_jobqueue(num_workers), False, signkey
    )


runner_factory = RunnerFactory()
runner_factory.register_builder("ZMQ_ACTOR_RUNNER", build_zmq_actor_runner)
runner_factory.register_builder("ZMQ_QUEUE_RUNNER", build_zmq_queue_runner)
runner_factory.register_builder("REDIS_QUEUE_RUNNER", build_redis_queue_runner)
runner_factory.register_builder("REDIS_ACTOR_RUNNER", build_redis_actor_runner)
runner_factory.register_builder(
    "AMQP_QUEUE_RUNNER", build_rabbitmq_queue_runner
)
runner_factory.register_builder(
    "AMQP_ACTOR_RUNNER", build_rabbitmq_actor_runner
)
