# -*- coding: utf-8 -*-

"""
tasq.jobqueue.py
~~~~~~~~~~~~~~~~
"""

from uuid import uuid4
from multiprocessing.queues import JoinableQueue
from multiprocessing import get_context

from job import Job
from worker import Worker


class JobQueue(JoinableQueue):

    def __init__(self, result_queue, num_workers=5):
        ctx = get_context('spawn')
        JoinableQueue.__init__(self, ctx=ctx)
        self.num_workers = num_workers
        self.result_queue = result_queue
        self.start_workers()

    @staticmethod
    def get_uuid():
        return uuid4()

    def add_task(self, task, *args, **kwargs):
        self.put(Job(JobQueue.get_uuid(), task, *args, **kwargs))

    def get_task(self):
        return self.get()

    def start_workers(self):
        for _ in range(self.num_workers):
            w = Worker(self)
            w.daemon = True
            w.start()
