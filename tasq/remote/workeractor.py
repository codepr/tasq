# -*- coding: utf-8 -*-

"""
tasq.remote.workeractor.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains all actors used as workers for all tasks incoming from remote calls.
"""

import time
from ..actor import Actor, Result


class WorkerActor(Actor):

    """Simple worker actor, execute a `job` and set a result with the response"""

    def submit(self, job):
        r = Result()
        self.send((job, r))
        return r

    def run(self):
        while True:
            job, result = self.recv()
            start_time = time.time()
            response = job.execute()
            self._log.debug("Job succesfully executed in %s s", time.time() - start_time)
            result.set_result(response)


class ResponseActor(Actor):

    """Response actor, it's task is to answer back to the client by leveragin the PUSH/PULL
    communication pattern offered by ZMQ sockets"""

    def submit(self, sendfunc, result):
        self.send((sendfunc, result))

    def run(self):
        while True:
            sendfunc, res = self.recv()
            sendfunc(res.result())
