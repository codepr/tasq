"""
tasq.remote.actors.py
~~~~~~~~~~~~~~~~~~~~~
This module contains all actors and routers as well used as workers for all
tasks incoming from remote calls.
"""
import asyncio
from concurrent.futures import Future
from ..actors.actor import Actor


class WorkerActor(Actor):

    """Simple worker actor, execute a `job` and set a result with the
    response

    Attributes
    ----------
    :type name: str or ''
    :param name: The name of the actor, this should uniquely identify it

    :type ctx: actorsystem.ActorSystem or None
    :param ctx: Context variable which can be used to spawn additional actors,
                generally this is the ActorSystem singleton which rules the
                entire fleet of actors.

    :type response_actor: tasq.actors.actor.Actor or None
    :param response_actor: Instance of an Actor responsible for communication
                           with the requesting client

    """

    def __init__(self, name="", ctx=None, response_actor=None):
        super().__init__(name, ctx)
        # self._response_actor = response_actor or ctx.actor_of(
        #     ResponseActor, f"ResponseActor - {name}"
        # )

    def submit(self, job):
        """Submits a job object to the run loop of the actor, returning
        immediatly a `Result` object without having to wait for it to be filled
        with the effective processing result of the job

        Args:
        -----
        :type job: tasq.Job
        :param job: The `tasq.Job` object containing the function to be
                    executed in the current actor

        :return: A `concurrent.Future` object, future object that will contain
                 the result of the job execution
        """
        self._log.debug(
            "Sending message to actor %s - pending jobs %s",
            self.name,
            self.mailbox_size,
        )
        f = asyncio.Future()
        self.send((job, f))
        return f

    def run(self):
        """Executes pending jobs, setting the results to the associated
        `Result` object once it is ready
        """
        while True:
            job, future = self.recv()
            self._log.debug("Received %s", job)
            # If eta in keyword arguments spawn a timed actor and send job to
            # it
            if "eta" in job.kwargs:
                eta = job.kwargs.pop("eta")
                timed_actor = self._ctx.actor_of(
                    TimedActor,
                    "TimedActor - " + job.job_id,
                    response_actor=self._response_actor,
                )
                timed_actor.start()
                timed_actor.submit(job, eta)
            else:
                self._log.debug("%s - executing job %s", self.name, job.job_id)
                response = job.execute()
                self._log.debug(
                    "%s - Job %s succesfully executed in %s s",
                    self.name,
                    job.job_id,
                    job.execution_time(),
                )
                if not response.value and response.exc:
                    jobres = response.exc
                else:
                    jobres = response.value
                self._log.debug(
                    "%s - Job %s result = %s", self.name, job.job_id, jobres
                )
                future.set_result(response)


class ResponseActor(Actor):

    """Response actor, it's task is to answer back to the client by leveraging
    the PUSH/PULL communication pattern offered by ZMQ sockets

    Attributes
    ----------
    :type name: str or ''
    :param name: The name of the actor, this should uniquely identify it

    :type ctx: actorsystem.ActorSystem or None
    :param ctx: Context variable which can be used to spawn additional actors,
                generally this is the ActorSystem singleton which rules the
                entire fleet of actors.

    :type response_actor: tasq.actors.actor.Actor or None
    :param response_actor: Instance of an Actor responsible for communication
                           with the requesting client

    :type sendfunc: function
    "param sendfunc: The sending function, as the respond-responsible function
                     to call to communicate results to a client

    """

    def __init__(self, name="", ctx=None, *, sendfunc=None):
        self._sendfunc = sendfunc
        super().__init__(name, ctx)

    def run(self):
        """Send response back to connected clients by using ZMQ PUSH channel"""
        while True:
            res = self.recv()
            self._sendfunc(res.result())


class TimedActor(Actor):

    """Actor designed to run only a single task every defined datetime"""

    def __init__(self, name="", ctx=None, response_actor=None):
        self._response_actor = response_actor or self._ctx.actor_of(
            ResponseActor, "TimedActor - ResponseActor"
        )
        if isinstance(self._response_actor, Actor):
            self._response_actor.start()
        super().__init__(name, ctx)

    def submit(self, job, eta):
        """Submit a time-scheduled job, to be executed every defined interval.
        Eta define the interval of the repeating task, and can be specified as
        an int, meaning seconds, or as string specifying the measure unit.

        E.g.

        4s -> Job executed every 4 seconds
        6m -> Job executed every 6 minutes
        8h -> Job executed every 8 hours

        Args:
        -----
        :type job: tasq.Job
        :param job: The `tasq.Job` object containing the function to be
                    executed in the current actor

        :type eta: str
        :param eta: The time that pass in every tic of the repeating interval

        :return: A `concurrent.Future` object, future object that will contain
                 the result of the job execution
        """
        future = Future()
        multiples = {"h": 60 * 60, "m": 60, "s": 1}
        if isinstance(eta, int):
            delay = eta
        else:
            try:
                delay = int(eta)
            except ValueError:
                delay = multiples[eta[-1]] * int(eta[:-1])
        self._log.debug(
            "Sending message to actor %s - pending jobs %s",
            self.name,
            self.mailbox_size,
        )
        job.add_delay(delay)
        self.send((job, future))
        return future

    def run(self):
        """Executes pending jobs, setting the results to the associated
        `Result` object once it is ready
        """
        while True:
            job, result = self.recv()
            self._log.debug("%s - executing timed job %s", self.name, job)
            response = job.execute()
            self._log.debug(
                "%s - Job % succesfully executed in %s s",
                self.name,
                job.job_id,
                job.execution_time(),
            )
            if not response.value and response.exc:
                jobres = response.exc
            else:
                jobres = response.value
            self._log.debug(
                "%s - Timed job %s result = %s", self.name, job.job_id, jobres
            )
            result.set_result(response)
            if isinstance(self._response_actor, Actor):
                self._response_actor.send(result)
            else:
                self._response_actor.route(result)
            self.submit(job, str(job.delay) + "s")


class ClientWorker(Actor):

    """Simplicistic worker with responsibility to communicate job scheduling to
    a `TasqClient` instance

    Attributes
    ----------
    :type name: str or ''
    :param name: The name of the actor, this should uniquely identify it

    :type ctx: actorsystem.ActorSystem or None
    :param ctx: Context variable which can be used to spawn additional actors,
                generally this is the ActorSystem singleton which rules the
                entire fleet of actors.

    """

    def __init__(self, client, name="", ctx=None):
        self._client = client
        super().__init__(name=name, ctx=ctx)

    def submit(self, job):
        """Create a `Future` object and enqueue it into the mailbox with the
        associated job, then return it to the caller

        Args:
        -----
        :type job: tasq.Job
        :param job: The `tasq.Job` object containing the function to be
                    executed in the current actor

        :return: A `concurrent.Future` object, future object that will contain
                 the result of the job execution
        """
        future = Future()
        self.send((job, future))
        return future

    def run(self):
        """Consumes all messages in the mailbox, setting each future's result
        with the result returned by the call.
        """
        while True:
            job, future = self.recv()
            self._log.debug("Received %s", job)
            self._log.debug("%s - executing job %s", self.name, job.job_id)
            if not self._client.is_connected:
                self._client.connect()
            fut = self._client.schedule(
                job.func, *job.args, name=job.job_id, **job.kwargs
            )
            # XXX A bit sloppy, but probably better schedule the fut result
            # settings in a callback
            future.set_result(fut.result())
