"""
tasq.actors.routers.py
~~~~~~~~~~~~~~~~~~~~~~
This module contains all routers used as workers for all tasks incoming from
remote calls.
"""
from abc import ABCMeta, abstractmethod


class Router(metaclass=ABCMeta):

    """Oversimplified router system to enroute messages to a pool of workers
    actor, by subclassing this it is possible to add different heuristic of
    message routing. If the len of the workers pool is just 1, ignore every
    defined heuristic and send the message to that only worker.

    Attributes
    ----------
    :type workers: int
    :param workers: Number of workers of the pool

    :type func_name: str or 'submit'
    :param func_name: The name of the method that must be called after the
                      message has been routed.

    """

    def __init__(self, workers, func_name="submit", *args, **kwargs):
        self._workers = workers
        self._func_name = func_name
        super().__init__(*args, **kwargs)

    @property
    def workers(self):
        return self._workers

    def _call_func(self, idx, msg):
        """Check if the defined `func_name` is present in the worker positioned
        at `idx` in the pool and call that function, passing in `msg`.
        """
        if hasattr(self._workers[idx], self._func_name):
            # Lazy check for actor state
            if not self._workers[idx].is_running:
                self._workers[idx].start()
            # Call the defined function to enqueue messages to the actor
            func = getattr(self._workers[idx], self._func_name)
            return func(msg)

    @abstractmethod
    def _route_message(self, msg):
        """To be defined on subclass"""
        raise NotImplementedError

    def route(self, msg):
        """Call `_route_message` private method, call function directly in case
        of a single worker pool
        """
        if len(self._workers) == 1:
            return self._call_func(0, msg)
        return self._route_message(msg)


class RoundRobinRouter(Router):

    """Round-robin heuristic of distribution of messages between workers"""

    def __init__(self, workers, func_name):
        super(RoundRobinRouter, self).__init__(workers, func_name)
        self._idx = 0

    def _route_message(self, msg):
        """Send message to the current indexed actor, if the index reach the
        max length of the actor list, it reset it to 0
        """
        if self._idx == len(self._workers) - 1:
            self._idx = 0
        else:
            self._idx += 1
        return self._call_func(self._idx, msg)


class RandomRouter(Router):

    """Select a random worker from the pool and send the message to it"""

    def _route_message(self, msg):
        """Retrieve a random index in the workers list and send message to the
        corresponding actor
        """
        import random

        idx = random.randint(0, len(self._workers))
        return self._call_func(idx, msg)


class SmallestMailboxRouter(Router):

    """Check which worker has minor load of messages in a really naive way and
    send the message to it
    """

    def _route_message(self, msg):
        """Create a dictionary formed by mailbox size of each actor as key, and
        the actor itself as value; by finding the minimum key in the
        dictionary, it send the message to the actor associated with.
        """
        actor = min(self._workers, key=lambda w: w.mailbox_size)
        idx = self._workers.index(actor)
        return self._call_func(idx, msg)


def actor_pool(
    num_workers, actor_class, router_class, func_name="submit", *args, **kwargs
):
    """Return a router object by getting the definition directly from the
    module, raising an exception if not found. Init the object with a
    `num_workers` actors
    """
    actorname = actor_class.__name__
    clients = kwargs.pop("clients", None)
    if clients:
        return router_class(
            [
                actor_class(name=f"{actorname}-{i}", client=c, *args, **kwargs)
                for i, c in zip(range(num_workers), clients)
            ],
            func_name,
        )
    return router_class(
        [
            actor_class(name=f"{actorname}-{i}", *args, **kwargs)
            for i in range(num_workers)
        ],
        func_name,
    )
