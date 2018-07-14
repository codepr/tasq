# -*- coding: utf-8 -*-

"""
tasq.worker.py
~~~~~~~~~~~~~~
Generic worker, useful to run jobs in a single node with multiple core available.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import uuid
import time
import signal
import logging
from threading import Thread
from multiprocessing import Process


_fmt = logging.Formatter('%(message)s', '%Y-%m-%d %H:%M:%S')


class Worker:

    """Generic worker class, contains a job queue and handle incoming jobs for execution"""

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
            self._last_job = job = self._job_queue.get_task()
            tic = time.time()
            self._completed_jobs.put(job.execute())
            self._done = True
            self._log.debug("Finished job in %s s", (time.time() - tic))

    def exit(self, sgl, frm):
        """Handle exit signals"""
        if self._done is False and self._last_job is not None:
            self._job_queue.put(self._last_job)
            self._log.debug("Re added interrupted job")
        sys.exit()


class ThreadWorker(Worker, Thread):

    """Generic worker process, contains a job queue and handle incoming jobs for execution"""

    def __init__(self, job_queue, name=u'', debug=False, daemon=True):
        super().__init__(job_queue, name, debug, daemon)


class ProcessWorker(Worker, Process):

    def __init__(self, job_queue, name=u'', debug=False, daemon=True):
        # Process name, defaulted to a uuid
        self._name = name or uuid.uuid4()
        # A joinable job queue
        self.job_queue = job_queue
        # Debug flag
        self._debug = debug
        super().__init__(job_queue, name, debug, daemon)

    def run(self):
        from .remote.sockets import decompress_and_unpickle
        while True:
            self._last_job = job = decompress_and_unpickle(self.job_queue.get())
            print(type(job))
            response = job.execute()
            self._completed_jobs.put(response)
