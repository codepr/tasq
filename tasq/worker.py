# -*- coding: utf-8 -*-

"""
tasq.worker.py
~~~~~~~~~~~~~~
"""

import sys
import time
import signal
import logging
from multiprocessing import Process


class Worker(Process):

    def __init__(self, job_queue):
        Process.__init__(self)
        self._job_queue = job_queue
        self._result_queue = self._job_queue.result_queue
        self._done = False
        self._last_job = None
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def run(self):
        while True:
            self._last_job = job = self._job_queue.get_task()
            start_time = time.time()
            self._result_queue.put(job.execute())
            self._done = True
            logging.error("Finished job in %s s", (time.time() - start_time))

    def exit(self, sgl, frm):
        if self._done is False and self._last_job is not None:
            self._job_queue.put(self._last_job)
            logging.error("Re added interrupted job")
        sys.exit()
