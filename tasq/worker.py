# -*- coding: utf-8 -*-

"""
tasq.worker.py
~~~~~~~~~~~~~~
Generic worker, useful to run jobs in a single node with multiple core available.
"""

import sys
import uuid
import time
import signal
import logging
from multiprocessing import Process


_formatter = logging.Formatter('%(levelname)s - %(processName)s - %(message)s', '%Y-%m-%d %H:%M:%S')


class Worker(Process):

    """Generic worker process, contains a job queue and handle incoming jobs for execution"""

    def __init__(self, job_queue, name=u'', debug=False):
        Process.__init__(self)
        # Process name, defaulted to a uuid
        self._name = name or uuid.uuid4()
        # A joinable job queue
        self._job_queue = job_queue
        # Contains results of jobs execution
        self._result_queue = self._job_queue.result_queue
        # Debug flag
        self._debug = debug
        # Last job done flag
        self._done = False
        # Track last job executed
        self._last_job = None
        # Logging settings
        self._log = logging.getLogger(f'{__name__}.{self._name}')
        sh = logging.StreamHandler()
        sh.setFormatter(_formatter)
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

    @property
    def name(self):
        return self._name

    @property
    def results(self):
        return self._result_queue

    @property
    def done(self):
        return self._done

    def run(self):
        """Start execution of all jobs in the jobqueue"""
        while True:
            self._last_job = job = self._job_queue.get_task()
            tic = time.time()
            self._result_queue.put(job.execute())
            self._done = True
            self._log.debug("Finished job in %s s", (time.time() - tic))

    def exit(self, sgl, frm):
        """Handle exit signals"""
        if self._done is False and self._last_job is not None:
            self._job_queue.put(self._last_job)
            self._log.debug("Re added interrupted job")
        sys.exit()
