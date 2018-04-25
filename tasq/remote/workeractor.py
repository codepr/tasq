# -*- coding: utf-8 -*-

"""
tasq.remote.workeractor.py
~~~~~~~~~~~~~~~~~~~~~~~~
"""

from ..actor import Actor, Result


class WorkerActor(Actor):

    def submit(self, job):
        r = Result()
        self.send((job, r))
        return r

    def run(self):
        while True:
            job, result = self.recv()
            response = job.execute()
            result.set_result(response)


class ResponseActor(Actor):

    def submit(self, sendfunc, result):
        self.send((sendfunc, result))

    def run(self):
        while True:
            sendfunc, res = self.recv()
            sendfunc(res.result())
