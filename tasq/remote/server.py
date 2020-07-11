"""
tasq.remote.server.py
~~~~~~~~~~~~~~~~~~~~~

This module contains classes to create servers.
"""

import zmq
from abc import ABC, abstractmethod
from .sockets import AsyncCloudPickleContext, CloudPickleContext


class Server(ABC):

    """Connection class, set up two communication channels, a PUSH one using a
    synchronous socket and a PULL one using an asynchronous socket. Each socket
    is a subclass of zmq sockets given the capability to handle cloudpickled
    data
    """

    def __init__(self, host, push_port, pull_port, signkey=None):
        # Host address to bind sockets to
        self._host = host
        # Send digital signed data
        self._signkey = signkey
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # ZMQ settings
        self._ctx = None
        self._push_socket = None
        self._pull_socket = None
        self._poller = None
        self._make_sockets()

    @abstractmethod
    def _make_sockets(self):
        pass

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor
        """
        self._pull_socket.bind(f"tcp://{self._host}:{self._pull_port}")
        self._push_socket.bind(f"tcp://{self._host}:{self._push_port}")

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx.destroy()


class ZMQTcpServer(Server):
    def _make_sockets(self):
        self._ctx = AsyncCloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)
        # ZMQ poller settings for async recv
        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._pull_socket, zmq.POLLIN)

    async def poll(self):
        events = await self._poller.poll()
        if self._pull_socket in dict(events):
            return dict(events)
        return None

    async def send(self, data, unpickle=True, flags=0):
        """Send data through the PUSH socket, if a signkey flag is set it sign
        it before sending
        """
        await self._push_socket.send_data(data, unpickle, flags, self._signkey)

    async def recv(self, unpickle=True, flags=0):
        """Asynchronous receive data from the PULL socket, if a signkey flag is
        set it checks for integrity of the received data
        """
        return await self._pull_socket.recv_data(unpickle, flags, self._signkey)


class ZMQUnixServer(ZMQTcpServer):

    """Connection class derived from MixedServer, which uses UNIX sockets
    instead of TCP sockets, pull and push port are used to define the name of
    the file descriptor instead of the address.

    e.g.
    PUSH unix socket: /tmp/supervisor-9091
    PULL unix socket: /tmp/supervisor-9092
    """

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address-port
        pairs defined in the constructor, being used UNIX sockets, push and
        pull ports are used to define file descriptor on the filesystem.
        """
        self._pull_socket.bind(f"ipc://{self._host}-{self._pull_port}")
        self._push_socket.bind(f"ipc://{self._host}-{self._push_port}")
