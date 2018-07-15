# -*- coding: utf-8 -*-

"""
tasq.jobqueue.py
~~~~~~~~~~~~~~~~
Contains naive implementation of a joinable queue for execution of tasks in a single node context.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from multiprocessing import get_context
from multiprocessing.queues import JoinableQueue

from .worker import ProcessQueueWorker


class JobQueue(JoinableQueue):

    """
    JoinableQueue subclass which spin a pool of workers to execute job in background, workers can
    be either threads or processes. The distinction can be assumed based on the nature of the tasks,
    being them more of I/O bound tasks or CPU bound tasks.
    """

    def __init__(self, completed_jobs, num_workers=8,
                 start_method='fork', worker_class=ProcessQueueWorker, debug=False):
        # Retrieve the spawn context for the joinable queue super class
        ctx = get_context(start_method)
        # Init super class
        super().__init__(ctx=ctx)
        # Number of workers to spawn
        self._num_workers = num_workers
        # JoinableQueue to store completed jobs
        self._completed_jobs = completed_jobs
        # Worker class, can be either Process or Thread
        self._workerclass = worker_class
        # Debug flag
        self._debug = debug
        # Spin the workers
        self.start_workers()

    @property
    def completed_jobs(self):
        return self._completed_jobs

    @property
    def num_workers(self):
        return self._num_workers

    def add_job(self, job):
        """Add a job to the queue to be executed

        Args:
        -----
        :type job: tasq.Job
        :param job: The `tasq.Job` object containing the function to be executed
        """
        self.put(job)

    def get_job(self):
        """Retrieve the last inserted job

        :return: A `tasq.Job` object
        """
        return self.get()

    def start_workers(self):
        """Create and start all the workers"""
        for _ in range(self.num_workers):
            w = self._workerclass(self, self.completed_jobs, debug=self._debug)
            w.start()
