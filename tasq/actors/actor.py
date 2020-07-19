"""
tasq.actors.actor.py
~~~~~~~~~~~~~~~~~~~~

Contains definitions of generic Actor class, must be subclassed to effectively
instance useful actors.
"""

import uuid
from queue import Queue
from threading import Thread, Event
from abc import ABC, abstractmethod

from ..logger import get_logger
from .actorsystem import ActorSystem


class ActorExit(Exception):
    pass


class Actor(ABC):

    """Class modeling a basic erlang-style actor, a simple object which can be
    used to push messages into a mailbox and process them in separate thread,
    concurrently, without sharing any state with other actors.

    Attributes
    ----------
    :type name: str or ''
    :param name: The name of the actor, this should uniquely identify it

    :type ctx: actorsystem.ActorSystem or None
    :param ctx: Context variable which can be used to spawn additional actors,
                generally this is the ActorSystem singleton which rules the
                entire fleet of actors.

    """

    def __init__(self, name="", ctx=None):
        # Assingn a default uuid name in case of no name set
        self._name = name or uuid.uuid4()
        self._is_running = False
        self._mailbox = Queue()
        self._terminated = Event()
        # XXX it is really a joke at the moment but still,
        # context == actorsystem singleton
        self._ctx = ctx or ActorSystem()
        # Logging settings
        self._log = get_logger(f"{__name__}.{self._name}")

    @property
    def name(self):
        return self._name

    @property
    def is_running(self):
        return self._is_running

    @property
    def mailbox_size(self):
        return self._mailbox.qsize()

    def send(self, msg):
        """Sends a message to the actor, effectively putting it into the
        mailbox
        """
        self._mailbox.put(msg)

    def recv(self):
        """Poll the mailbox for pending messages, blocking if empty. In case of
        `ActorExit` message it raises an execption and shutdown the actor
        loop
        """
        msg = self._mailbox.get()
        if msg is ActorExit:
            self._is_running = False
            raise ActorExit()
        return msg

    def close(self):
        """Shutdown the actor by sending an `ActorExit` to the mailbox"""
        self.send(ActorExit)

    def start(self):
        """Run the processing thread"""
        t = Thread(target=self._bootstrap, daemon=True)
        t.start()
        self._is_running = True
        self._log.debug("%s started", self.name)

    def _bootstrap(self):
        """Target method to be run into a thread, call the `run` method till an
        `ActorExit` message
        """
        try:
            self.run()
        except ActorExit:
            pass
        finally:
            self._terminated.set()

    def join(self):
        """Wait till the end of all messages"""
        self._terminated.wait()

    @abstractmethod
    def run(self):
        """Must be implemented by subclasses"""
        raise NotImplementedError
