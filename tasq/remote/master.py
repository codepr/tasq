# -*- coding: utf-8 -*-

"""
tasq.remote.master.py
~~~~~~~~~~~~~~~~~~~~~
Master process, listening for incoming connections to schedule tasks to a pool of worker actors
"""

import signal
import asyncio
import logging
from multiprocessing import Process
import zmq

from .actors import ResponseActor, actor_pool
from .sockets import AsyncCloudPickleContext, CloudPickleContext


_formatter = logging.Formatter('%(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')


class Master:

    """
    Master process, handle requests asynchronously from clients and delegate processing of
    incoming tasks to worker actors, responses are sent back to clients by using a pool of actors as
    well
    """

    def __init__(self, host, push_port, pull_port, num_workers=5, debug=False):
        # Host address to bind sockets to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Number of workers
        self._num_workers = num_workers
        # Debug flag
        self._debug = debug
        # Logging settings
        self._log = logging.getLogger(f'{__name__}.{self._host}.{self._push_port}')
        sh = logging.StreamHandler()
        sh.setFormatter(_formatter)
        if self._debug is True:
            sh.setLevel(logging.DEBUG)
            self._log.setLevel(logging.DEBUG)
            self._log.addHandler(sh)
        else:
            sh.setLevel(logging.INFO)
            self._log.setLevel(logging.INFO)
            self._log.addHandler(sh)
        # ZMQ settings
        self._context = CloudPickleContext()
        self._async_context = AsyncCloudPickleContext()
        self._push_socket = self._async_context.socket(zmq.PULL)
        self._pull_socket = self._context.socket(zmq.PUSH)
        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._push_socket, zmq.POLLIN)
        # Generic worker actor
        self._responses = ResponseActor(name=u'Response actor', debug=self._debug)
        # Actor for responses
        self._workers = actor_pool(
            self._num_workers,
            actor_class='WorkerActor',
            routing_type='SmallestMailboxRouter',
            debug=self._debug
        )
        # Event loop
        self._loop = asyncio.get_event_loop()
        # Handling loop exit
        self._loop.add_signal_handler(signal.SIGINT, self._stop)

    @property
    def host(self):
        return self._host

    @property
    def push_port(self):
        return self._push_port

    @property
    def pull_port(self):
        return self._pull_port

    @property
    def num_workers(self):
        return self._num_workers

    def _bind_sockets(self):
        """Binds PUSH and PULL channel sockets to the respective address:port pairs defined in the
        constructor"""
        self._push_socket.bind(f'tcp://{self._host}:{self._push_port}')
        self._pull_socket.bind(f'tcp://{self._host}:{self._pull_port}')
        self._log.debug("Push channel set to %s:%s", self._host, self._push_port)
        self._log.debug("Pull channel set to %s:%s", self._host, self._pull_port)

    async def _start(self):
        """Receive jobs from clients with polling"""
        while True:
            events = await self._poller.poll()
            if self._push_socket in dict(events):
                job = await self._push_socket.recv_data()
                res = self._workers.route(job)
                self._responses.submit(self._pull_socket.send_data, res)

    def _stop(self):
        """Stops the loop after canceling all remaining tasks"""
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self._loop.stop()
        print("\nCtrl+C again to exit")

    def serve_forever(self):
        """Blocking function, schedule the execution of the coroutine waiting for incoming tasks and
        run the asyncio loop forever"""
        self._bind_sockets()
        self._responses.start()
        asyncio.ensure_future(self._start())
        self._loop.run_forever()


class Masters:

    def __init__(self, binds, debug=False):
        self._binds = binds
        self._debug = debug
        self._procs = []
        self._init_binds()

    def _init_binds(self):
        self._procs = [
            Process(
                target=Master(host, psh_port, pl_port, debug=self._debug).serve_forever(),
                daemon=True
            ) for host, psh_port, pl_port in self._binds
        ]

    def start_procs(self):
        for proc in self._procs:
            proc.start()
