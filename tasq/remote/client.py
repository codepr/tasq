# -*- coding: utf-8 -*-

"""
tasq.remote.client.py
~~~~~~~~~~~~~~~~~~~~~
Client part of the application, responsible for scheduling jobs to local or remote workers.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import time
from threading import Thread
from collections import deque

import zmq

from ..job import Job
from .sockets import CloudPickleContext


class TasqClientNotConnected(Exception):

    def __init__(self, msg=u''):
        self.message = msg
        super().__init__(self.message)


class TasqClient:

    """Simple client class to schedule jobs to remote workers, currently supports a synchronous way
    of calling tasks awaiting for results and an asynchronous one which collect results in a
    dedicated dictionary"""

    def __init__(self, host, port, plport=None):
        # Host address of a remote master to connect to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._port = port
        # Pull port for ingoing messages, containing result data
        self._plport = plport or port + 1
        # Connection flag
        self._is_connected = False
        # ZMQ settings
        self._context = CloudPickleContext()
        self._task_socket = self._context.socket(zmq.PUSH)
        self._recv_socket = self._context.socket(zmq.PULL)
        # Results dictionary, mapping task_name -> result
        self._results = {}
        # Pending requests while not connected
        self._pending = deque()
        # Gathering results, making the client unblocking
        self._gatherer = Thread(target=self._gather_results, daemon=True)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def plport(self):
        return self._plport

    @property
    def is_connected(self):
        return self._is_connected

    @property
    def pending(self):
        return self._pending

    @property
    def results(self):
        return self._results

    def _gather_results(self):
        """Gathering subroutine, must be run in another thread to concurrently listen for results
        and store them into a dedicated dictionary"""
        while True:
            job_result = self._recv_socket.recv_data()
            if not job_result.value and job_result.exc:
                self._results[job_result.name] = job_result.exc
            else:
                self._results[job_result.name] = job_result.value

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels, respectively used to
        send tasks and to retrieve results back"""
        if not self.is_connected:
            self._task_socket.connect(f'tcp://{self._host}:{self._port}')
            self._recv_socket.connect(f'tcp://{self._host}:{self._plport}')
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
            self._task_socket.disconnect(f'tcp://{self._host}:{self._port}')
            self._recv_socket.disconnect(f'tcp://{self._host}:{self._plport}')
            self._is_connected = False

    def close(self):
        """Close sockets connected to workers, destroy zmq cotext"""
        if self.is_connected:
            self.disconnect()
        self._task_socket.close()
        self._recv_socket.close()
        self._context.destroy()

    def schedule(self, runnable, *args, **kwargs):
        """Schedule a job to a remote worker, without blocking. Require a runnable task, and
        arguments to be passed with, cloudpickle will handle dependencies shipping. Optional it is
        possible to give a name to the job, otherwise a UUID will be defined"""
        name = kwargs.pop('name', u'')
        job = Job(name, runnable, *args, **kwargs)
        if not self.is_connected:
            self._pending.appendleft(job)
        else:
            self._task_socket.send_data(job)

    def schedule_blocking(self, runnable, *args, **kwargs):
        """Schedule a job to a remote worker wating for the result to be ready. Like `schedule` it
        require a runnable task, and arguments to be passed with, cloudpickle will handle
        dependencies shipping. Optional it is possible to give a name to the job, otherwise a UUID
        will be defined"""
        if not self.is_connected:
            raise TasqClientNotConnected('Client not connected to no worker')
        name = kwargs.pop('name', u'')
        timeout = kwargs.pop('timeout', None)
        job = Job(name, runnable, *args, **kwargs)
        # Make sure that we not return a previous result
        if job.job_id in self.results:
            del self._results[job.job_id]
        # Actually make the call
        self._task_socket.send_data(job)
        # Poor timeout ticker
        tic = int(time.time())
        while True:
            if job.job_id in self.results:
                return job.job_id, self.results[job.job_id]
            # Check if timeout expired
            if timeout and (int(time.time()) - tic) >= timeout:
                break


class TasqClientPool:

    """Basic client pool, defining methods to talk to multiple remote workers"""

    # TODO WIP - still a rudimentary implementation

    def __init__(self, config, debug=False):
        # Debug flag
        self._debug = debug
        # List of tuples defining host:port pairs to connect
        self._config = config
        # Pool of clients
        self._clients = [TasqClient(host, psport, plport) for host, psport, plport in self._config]
        # Collect results, list of dictionaries
        self._results = []

    @property
    def results(self):
        """Lazily check for new results and add them to the list of dictionaries before returning
        it"""
        for client in self._clients:
            self._results.append(client.results)
        return self._results

    def map(self, func, iterable):
        idx = 0
        for args, kwargs in iterable:
            if idx == len(self._clients) - 1:
                idx = 0
            # Lazy check for connection
            if not self._clients[idx].is_connected:
                self._clients[idx].connect()
            self._clients[idx].schedule(func, *args, **kwargs)
