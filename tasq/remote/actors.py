# -*- coding: utf-8 -*-

"""
tasq.remote.actors.py
~~~~~~~~~~~~~~~~~~~~~
This module contains all actors and routers as well used as workers for all tasks incoming from
remote calls.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from ..actors.actor import Actor, Result


class WorkerActor(Actor):

    """Simple worker actor, execute a `job` and set a result with the response"""

    def __init__(self, name=u'', ctx=None, response_actor=None, debug=False):
        self._response_actor = response_actor or ctx.actor_of(
            ResponseActor, f'ResponseActor - {name}'
        )
        super().__init__(name, ctx, debug)

    def submit(self, job):
        """Submits a job object to the run loop of the actor, returning immediatly a `Result` object
        without having to wait for it to be filled with the effective processing result of the
        job"""
        self._log.debug(
            "Sending message to actor (%s | pending jobs %s)",
            self.name,
            self.mailbox_size
        )
        r = Result()
        self.send((job, r))
        return r

    def run(self):
        """Executes pending jobs, setting the results to the associated `Result` object once it is
        ready"""
        while True:
            job, result = self.recv()
            # If eta in keyword arguments spawn a timed actor and send job to it
            if 'eta' in job.kwargs:
                eta = job.kwargs.pop('eta')
                timed_actor = self._ctx.actor_of(
                    TimedActor,
                    'TimedActor - ' + job.job_id,
                    response_actor=self._response_actor,
                    debug=self._debug
                )
                timed_actor.start()
                timed_actor.submit(job, eta)
            else:
                self._log.debug("%s - executing job %s", self.name, job.job_id)
                response = job.execute()
                self._log.debug(
                    "%s - Job succesfully executed in %s s",
                    self.name,
                    job.execution_time()
                )
                result.set_result(response)


class ResponseActor(Actor):

    """Response actor, it's task is to answer back to the client by leveragin the PUSH/PULL
    communication pattern offered by ZMQ sockets"""

    def __init__(self, name=u'', ctx=None, debug=False, *, sendfunc=None):
        self._sendfunc = sendfunc
        super().__init__(name, ctx, debug)

    def submit(self, result):
        self.send(result)

    def run(self):
        """Send response back to connected clients by using ZMQ PUSH channel"""
        while True:
            res = self.recv()
            self._sendfunc(res.result())


class TimedActor(Actor):

    """Actor designed to run only a single task every defined datetime"""

    def __init__(self, name=u'', ctx=None, response_actor=None, debug=False):
        self._response_actor = response_actor or self._ctx.actor_of(
            ResponseActor, 'TimedActor - ResponseActor', debug
        )
        self._response_actor.start()
        super().__init__(name, ctx, debug)

    def submit(self, job, eta):
        """Submit a time-scheduled job, to be executed every defined interval. Eta define the
        interval of the repeating task, and can be specified as an int, meaning seconds, or as
        string speicifying the measure unit. E.g.
        4s -> Job executed every 4 seconds
        6m -> Job executed every 6 minutes
        8h -> Job executed every 8 hours
        """
        result = Result()
        multiples = {'h': 60 * 60, 'm': 60, 's': 1}
        if isinstance(eta, int):
            delay = eta
        else:
            try:
                delay = int(eta)
            except ValueError:
                delay = multiples[eta[-1]] * int(eta[:-1])
        self._log.debug(
            "Sending message to actor (%s | pending jobs %s)",
            self.name,
            self.mailbox_size
        )
        job.add_delay(delay)
        self.send((job, result))

    def run(self):
        """Executes pending jobs, setting the results to the associated `Result` object once it is
        ready"""
        while True:
            job, result = self.recv()
            self._log.debug("%s - executing job %s", self.name, job.job_id)
            response = job.execute()
            self._log.debug(
                "%s - Job succesfully executed in %s s",
                self.name,
                job.execution_time()
            )
            result.set_result(response)
            self._response_actor.submit(result)
            self.submit(job, str(job.delay) + 's')
