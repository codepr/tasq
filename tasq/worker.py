# -*- coding: utf-8 -*-

"""
tasq.worker.py
~~~~~~~~~~~~~~
Generic worker, useful to run jobs in a single node with multiple core available.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import uuid
import signal
import logging
from threading import Thread
from multiprocessing import Process

from .remote.sockets import decompress_and_unpickle


_fmt = logging.Formatter('%(message)s', '%Y-%m-%d %H:%M:%S')


class Worker:

    """Generic worker class, contains a job queue and handle incoming jobs for execution, should be
    mixed int with Thread or Process class"""

    def __init__(self, job_queue, completed_jobs, name=u'', debug=False, *args, **kwargs):
        # Process name, defaulted to a uuid
        self._name = name or uuid.uuid4()
        # A joinable job queue
        self._job_queue = job_queue
        # Completed jobs queue
        self._completed_jobs = completed_jobs
        # Debug flag
        self._debug = debug
        # Last job done flag
        self._done = False
        # Track last job executed
        self._last_job = None
        # Logging settings
        self._log = logging.getLogger(f'{__name__}.{self._name}')
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
        # Handle exit gracefully
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)
        super().__init__(*args, **kwargs)

    @property
    def name(self):
        return self._name

    @property
    def results(self):
        return self._completed_jobs

    @property
    def done(self):
        return self._done

    def run(self):
        """Start execution of all jobs in the job_queue"""
        while True:
            self._done = False
            self._last_job = None
            job = self._job_queue.get_job()
            self._last_job = job
            # Check for poison pill
            if job is None:
                break
            self._log.debug("%s - executing job %s", self.name, job.job_id)
            self._completed_jobs.put(job.execute())
            self._log.debug(
                "%s - Job %s succesfully executed in %s s",
                self.name,
                job.job_id,
                job.execution_time()
            )
            self._job_queue.task_done()
            self._done = True

    def exit(self, sgl, frm):
        """Handle exit signals"""
        if not self._done and self._last_job is not None:
            self._job_queue.put(self._last_job)
            self._log.debug("%s - Re added interrupted job", self.name)
        self._log.debug("%s - Exiting", self.name)
        self._job_queue.put(None)
        sys.exit()


class ThreadWorker(Worker, Thread):

    """Worker unit based on `threading.Thread` superclass, useful if the majority of the jobs are
    I/O bound"""

    pass


class ProcessWorker(Worker, Process):

    """Worker unit based on `multiprocessing.Process` superclass, meant to be employed in case the
    majority of the jobs are CPU bound"""

    def run(self):
        while True:
            # Need to decompress and unpickle data here cause the function contained in the job
            # could be not defined in the __main__ module being the worker optionally run in a
            # remote machine
            self._done = False
            self._last_job = None
            job = decompress_and_unpickle(self._job_queue.get())
            self._last_job = job
            self._log.debug("%s - executing job %s", self.name, job.job_id)
            response = job.execute()
            self._job_queue.task_done()
            self._log.debug(
                "%s - Job %s succesfully executed in %s s",
                self.name,
                job.job_id,
                job.execution_time()
            )
            # Push the completed job in the result queue ready to be answered to the requesting
            # client
            self._completed_jobs.put(response)
