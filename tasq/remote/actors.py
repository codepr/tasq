# -*- coding: utf-8 -*-

"""
tasq.remote.actors.py
~~~~~~~~~~~~~~~~~~~~~
This module contains all actors and routers as well used as workers for all tasks incoming from
remote calls.
"""

import time
from ..job import JobResult
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
            tic = time.time()
            response = job.execute()
            self._log.debug(
                "%s - Job succesfully executed in %s s",
                self.name,
                time.time() - tic
            )
            result.set_result(JobResult(job.job_id, response))


class ResponseActor(Actor):

    """Response actor, it's task is to answer back to the client by leveragin the PUSH/PULL
    communication pattern offered by ZMQ sockets"""

    def submit(self, sendfunc, result):
        self.send((sendfunc, result))

    def run(self):
        while True:
            sendfunc, res = self.recv()
            sendfunc(res.result())


class Router:

    """Oversimplified router system to enroute messages to a pool of workers actor, by subclassing
    this it is possible to add different heuristic of message routing. If the len of the workers
    pool is just 1, ignore every defined heuristic and send the message to that only worker."""

    def __init__(self, workers, func_name=u'send'):
        self._workers = workers
        self._func_name = func_name

    def _call_func(self, idx, msg):
        """Check if the defined `func_name` is present in the worker positioned at `idx` in the pool
        and call that function, passing in `msg`."""
        if hasattr(self._workers[idx], self._func_name):
            # Lazy check for actor state
            if not self._workers[idx].is_running:
                self._workers[idx].start()
            # Call the defined function to enqueue messages to the actor
            func = getattr(self._workers[idx], self._func_name)
            return func(msg)

    def _route_message(self, msg):
        """To be defined on subclass"""
        pass

    def route(self, msg):
        """Call `_route_message` private method, call function directly in case of a single worker
        pool"""
        if len(self._workers) == 1:
            return self._call_func(0, msg)
        return self._route_message(msg)


class RoundRobinRouter(Router):

    """Round-robin heuristic of distribution of messages between workers"""

    def __init__(self, workers, func_name):
        super(RoundRobinRouter, self).__init__(workers, func_name)
        self._idx = 0

    def _route_message(self, msg):
        if self._idx == len(self._workers) - 1:
            self._idx = 0
        else:
            self._idx += 1
        return self._call_func(self._idx, msg)


class RandomRouter(Router):

    """Select a random worker from the pool and send the message to it"""

    def _route_message(self, msg):
        import random
        idx = random.randint(0, len(self._workers))
        return self._call_func(idx, msg)


class SmallestMailboxRouter(Router):

    """Check which worker has minor load of messages in a really naive way and send the message to
    it"""

    def _route_message(self, msg):
        idx = min(w.mailbox_size for w in self._workers)
        print(f"Scheduled to worker - {idx}")
        return self._call_func(idx, msg)


def get_workers_pool(num_workers, routing_type='RoundRobinRouter', debug=False):
    import sys
    cls = getattr(sys.modules[__name__], routing_type)
    return cls([WorkerActor(name=f'Worker-{i}', debug=debug) for i in range(num_workers)], 'submit')
