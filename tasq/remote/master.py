# -*- coding: utf-8 -*-

"""
tasq.remote.master.py
~~~~~~~~~~~~~~~~~~~~~
Master process, listening for incoming connections to schedule tasks to a pool of worker actors
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import signal
import asyncio
import logging
from multiprocessing import Process
import zmq

from .actors import ResponseActor, WorkerActor
from .connection import ConnectionFactory
from ..actors.actorsystem import ActorSystem
from ..actors.routers import RoundRobinRouter, SmallestMailboxRouter


_fmt = logging.Formatter('%(message)s', '%Y-%m-%d %H:%M:%S')


class Master:

    """
    Master process, handle requests asynchronously from clients and delegate processing of
    incoming tasks to worker actors, responses are sent back to clients by using a pool of actors as
    well
    """

    def __init__(self, host, pull_port, push_port, num_workers=5,
                 router_class=RoundRobinRouter, sign_data=False, unix_socket=False, debug=False):
        # Host address to bind sockets to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Number of workers
        self._num_workers = num_workers
        # Routing type
        self._router_class = router_class
        # Send digital signed data
        self._sign_data = sign_data
        # Unix socket flag, if set to true, unix sockets for interprocess communication will be used
        # and ports will be used to differentiate push and pull channel
        self._unix_socket = unix_socket
        # Server reference to set up the communication
        self._server = ConnectionFactory \
            .make_server(host, push_port, pull_port, sign_data, unix_socket)
        # Debug flag
        self._debug = debug
        # Worker's ActorSystem
        self._system = ActorSystem(
            f'{self._host}:({self._push_port}, {self._pull_port})',
            self._debug
        )
        # Logging settings
        self._log = logging.getLogger(f'{__name__}.{self._host}.{self._push_port}')
        sh = logging.StreamHandler()
        sh.setFormatter(_fmt)
        if self._debug is True:
            sh.setLevel(logging.DEBUG)
            self._log.setLevel(logging.DEBUG)
            self._log.addHandler(sh)
        else:
            sh.setLevel(logging.INFO)
            self._log.setLevel(logging.INFO)
            self._log.addHandler(sh)
        # ZMQ poller settings for async recv
        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._server.pull_socket, zmq.POLLIN)
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

    @property
    def router_class(self):
        return self._router_class

    @property
    def unix_socket(self):
        return self._unix_socket

    def _bind_sockets(self):
        """Binds PUSH and PULL channel sockets to the respective address:port pairs defined in the
        constructor"""
        self._server.bind()
        self._log.info("Listening for jobs on %s:%s", self._host, self._pull_port)

    async def _start(self):
        """Receive jobs from clients with polling"""
        while True:
            events = await self._poller.poll()
            if self._server._pull_socket in dict(events):
                job = await self._server.recv()
                res = self._workers.route(job)
                self._responses.route(res)

    def _stop(self):
        """Stops the loop after canceling all remaining tasks"""
        print("\nStopping..")
        # Cancel pending tasks (opt)
        for task in asyncio.Task.all_tasks():
            task.cancel()
        # Stop the running loop
        self._loop.stop()
        # Stop server connection
        self._server.stop()

    def serve_forever(self):
        """Blocking function, schedule the execution of the coroutine waiting for incoming tasks and
        run the asyncio loop forever"""
        self._bind_sockets()
        asyncio.ensure_future(self._start())
        self._loop.run_forever()


class Masters:

    """Class to handle a pool of masters on the same node"""

    def __init__(self, binds, sign_data=False, unix_socket=False, debug=False):
        # List of tuples (host, pport, plport) to bind to
        self._binds = binds
        # Digital sign data before send an receive it
        self._sign_data = sign_data
        # Unix socket flag, if set to true, unix sockets for interprocess communication will be used
        # and ports will be used to differentiate push and pull channel
        self._unix_socket = unix_socket
        # Debug flag
        self._debug = debug
        # Processes, equals the len of `binds`
        self._procs = []
        self._init_binds()

    def _serve_master(self, host, psh_port, pl_port):
        m = Master(host, psh_port, pl_port, sign_data=self._sign_data,
                   unix_socket=self._unix_socket, debug=self._debug)
        m.serve_forever()

    def _init_binds(self):
        self._procs = [
            Process(
                target=self._serve_master,
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
