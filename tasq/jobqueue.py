# -*- coding: utf-8 -*-

"""
tasq.jobqueue.py
~~~~~~~~~~~~~~~~
Contains naive implementation of a joinable queue for execution of tasks in a single node context.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from multiprocessing import get_context
from multiprocessing.queues import JoinableQueue

from .job import Job
from .worker import ProcessWorker


class JobQueue(JoinableQueue):

    def __init__(self, completed_jobs, num_workers=5, start_method='fork'):
        ctx = get_context(start_method)
        super().__init__(ctx=ctx)
        self._num_workers = num_workers
        self._completed_jobs = completed_jobs
        self.start_workers()

    @property
    def completed_jobs(self):
        return self._completed_jobs

    @property
    def num_workers(self):
        return self._num_workers

    def add_task(self, job):
        self.put(job)

    def get_task(self):
        return self.get()

    def start_workers(self):
        for _ in range(self.num_workers):
            w = ProcessWorker(self, self.completed_jobs)
            w.start()
