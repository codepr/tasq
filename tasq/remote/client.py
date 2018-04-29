# -*- coding: utf-8 -*-

"""
tasq.remote.client.py
~~~~~~~~~~~~~~~~~~~~~
Client part of the application, responsible for scheduling jobs to local or remote workers.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import time
from threading import Thread

import zmq

from ..job import Job
from .sockets import CloudPickleContext


class TasqClient:

    """Simple client class to schedule jobs to remote workers, currently supports a synchronous way
    of calling tasks awaiting for results and an asynchronous one which collect results in a
    dedicated dictionary"""

    def __init__(self, host, port):
        # Host address of a remote master to connect to
        self._host = host
        # Port for push side (outgoing) of the communication channel
        self._port = port
        # ZMQ settings
        self._context = CloudPickleContext()
        self._task_socket = self._context.socket(zmq.PUSH)
        self._recv_socket = self._context.socket(zmq.PULL)
        # Results dictionary, mapping task_name -> result
        self._results = {}
        # Gathering results, making the client unblocking
        self._gatherer = Thread(target=self._gather, daemon=True)
        self._gatherer.start()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def results(self):
        return self._results

    def _gather(self):
        """Gathering subroutine, must be run in another thread to concurrently listen for results
        and store them into a dedicated dictionary"""
        while True:
            job_result = self._recv_socket.recv_data()
            self._results[job_result.name] = job_result.value

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels, respectively used to
        send tasks and to retrieve results back"""
        self._task_socket.connect(f'tcp://{self._host}:{self._port}')
        self._recv_socket.connect(f'tcp://{self._host}:{self._port + 1}')

    def schedule(self, runnable, *args, **kwargs):
        """Schedule a job to a remote worker, without blocking. Require a runnable task, and arguments
        to be passed with, cloudpickle will handle dependencies shipping. Optional it is possible to
        give a name to the job, otherwise a UUID will be defined"""
        name = kwargs.pop('name', u'')
        job = Job(name, runnable, *args, **kwargs)
        self._task_socket.send_data(job)

    def schedule_blocking(self, runnable, *args, **kwargs):
        """Schedule a job to a remote worker wating for the result to be ready. Like `schedule` it
        require a runnable task, and arguments to be passed with, cloudpickle will handle
        dependencies shipping. Optional it is possible to give a name to the job, otherwise a UUID
        will be defined"""
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
