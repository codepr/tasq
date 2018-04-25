# -*- coding: utf-8 -*-

"""
tasq.remote.remoteworker.py
~~~~~~~~~~~~~~~~~~~~
"""

import pickle
import logging
from multiprocessing import Process

import zmq
import cloudpickle

from ..job import Job
from .workeractor import WorkerActor, ResponseActor


class RemoteWorker:

    def __init__(self, host, push_port, pull_port):
        # Host address to bind sockets to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        self._context = zmq.Context()
        self._push_socket = self._context.socket(zmq.PULL)
        self._pull_socket = self._context.socket(zmq.PUSH)
        # Actor for job execution
        self._responses = ResponseActor()
        # Actor for responses
        self._worker = WorkerActor()

    def _bind_sockets(self):
        self._push_socket.bind('tcp://{}:{}'.format(self._host, self._push_port))
        self._pull_socket.bind('tcp://{}:{}'.format(self._host, self._pull_port))

    def start(self):
        self._bind_sockets()
        self._worker.start()
        self._responses.start()
        while True:
            runnable, args, kwargs = pickle.loads(self._push_socket.recv_pyobj())
            response = self._worker.submit(Job('', runnable, *args, **kwargs))
            self._responses.submit(self._pull_socket.send_pyobj, response)


class TasqClient:

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._zmq_context = zmq.Context()
        self._task_socket = self._zmq_context.socket(zmq.PUSH)
        self._recv_socket = self._zmq_context.socket(zmq.PULL)

    def connect(self):
        self._task_socket.connect('tcp://{}:{}'.format(self._host, self._port))
        self._recv_socket.connect('tcp://{}:{}'.format(self._host, self._port + 1))

    def schedule(self, runnable, *args, **kwargs):
        self._task_socket.send_pyobj(cloudpickle.dumps((runnable, args, kwargs)))
        results = self._recv_socket.recv_pyobj()
        return results


class RemoteWorkers:

    def __init__(self, binds):
        self._binds = binds
        self._procs = []
        self._init_binds()

    def _init_binds(self):
        self._procs = [
            Process(
                target=RemoteWorker(host, psh_port, pl_port).start(),
                daemon=True
            ) for host, psh_port, pl_port in self._binds
        ]

    def start_procs(self):
        for proc in self._procs:
            proc.start()
