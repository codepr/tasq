# -*- coding: utf-8 -*-

"""
tasq.jobqueue.py
~~~~~~~~~~~~~~~~
Contains naive implementation of a joinable queue for execution of tasks in a single node context.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from queue import Queue

from .job import Job
from .worker import Worker


class LocalJobQueue(Queue):

    """Queue for multithreaded execution of task on a single node"""

    def __init__(self, result_queue, num_workers=5):
        Queue.__init__(self)
        self._num_workers = num_workers
        self._result_queue = result_queue
        self._start_workers()

    @property
    def result_queue(self):
        return self._result_queue

    @property
    def num_workers(self):
        return self._num_workers

    def add_task(self, task, *args, **kwargs):
        name = kwargs.get('name', None)
        self.put(Job(name, task, *args, **kwargs))

    def get_task(self):
        return self.get()

    def _start_workers(self):
        for _ in range(self.num_workers):
            w = Worker(self)
            w.daemon = True
            w.start()
