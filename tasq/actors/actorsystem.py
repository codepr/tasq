"""
tasq.actors.actorsystem.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Contains definitions of a trivial Actor System class, useful to manage actor
creation and usable as context creator for child actors.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import uuid

from .routers import actor_pool
from ..util import SingletonArgs


class ActorSystem(metaclass=SingletonArgs):

    """Currently designed as a Singleton"""

    def __init__(self, name=u''):
        self._name = name or uuid.uuid4()
        self._actors = {}  # Mapping actor_name -> actor_ref

    @property
    def name(self):
        return self._name

    @property
    def actors(self):
        return self._actors

    def actor_of(self, actorclass, name=u'', *args, **kwargs):
        """Return the reference of an actor based on the actor class passed"""
        actor = actorclass(name, ctx=self, *args, **kwargs)
        self._actors[actor.name] = actor
        return actor

    def router_of(self, router_class, actor_class, num_workers,
                  func_name='submit', *args, **kwargs):
        """Return a router class type with the number and type of workers
        specified
        """
        pool = actor_pool(num_workers, actor_class, router_class,
                          func_name, *args, **kwargs)
        self._actors.update({w.name: w for w in pool.workers})
        return pool

    def shutdown(self):
        """Close all actors in the system"""
        for actor in self._actors.values():
            actor.close()

    def tell(self, actor_name, msg):
        """Send a message to a named actor in a fire-and-forget manner"""
        self._actors[actor_name].send(msg)

    def ask(self, actor_name, msg, timeout=None):
        # TODO
        pass
