# -*- coding: utf-8 -*-

"""
tasq.remote.connectio.py
~~~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to define connections using zmq sockets.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import zmq
from .sockets import AsyncCloudPickleContext, CloudPickleContext


class Server:

    """
    Connection class, set up two communication channels, a PUSH one using a synchronous socket and a
    PULL one using an asynchronous socket. Each socket is a subclass of zmq sockets given the
    capability to handle cloudpickled data
    """

    def __init__(self, host, push_port, pull_port, secure=False):
        # Host address to bind sockets to
        self._host = host
        # Send digital signed data
        self._secure = secure
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # ZMQ settings
        self._ctx = CloudPickleContext()
        self._actx = AsyncCloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._actx.socket(zmq.PULL)

    @property
    def push_socket(self):
        return self._push_socket

    @property
    def pull_socket(self):
        return self._pull_socket

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address:port pairs defined in the
        constructor"""
        self._pull_socket.bind(f'tcp://{self._host}:{self._pull_port}')
        self._push_socket.bind(f'tcp://{self._host}:{self._push_port}')

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx.destroy()
        self._actx.destroy()

    def send(self, data, flags=0):
        """Send data through the PUSH socket, if a secure flag is set it sign it before sending"""
        if self._secure is False:
            self._push_socket.send_data(data, flags)
        else:
            self._push_socket.send_signed(data, flags)

    async def recv(self, flags=0):
        """Asynchronous receive data from the PULL socket, if a secure flag is set it checks for
        integrity of the received data"""
        if self._secure is False:
            return await self._pull_socket.recv_data(flags)
        return await self._pull_socket.socket.recv_signed(flags)


class UnixServer(Server):

    """
    Connection class derived from Server, which uses UNIX sockets instead of TCP sockets, pull and
    push port are used to define the name of the file descriptor instead of the address.

    e.g.
    PUSH unix socket: /tmp/master-9091
    PULL unix socket: /tmp/master-9092
    """

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address-port pairs defined in the
        constructor, being used UNIX sockets, push and pull ports are used to define file descriptor
        on the filesystem."""
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')


class Connection:

    """
    Connection class, set up two communication channels, a PUSH and a PULL channel using two
    synchronous TCP sockets. Each socket is a subclass of zmq sockets given the capability to handle
    cloudpickled data
    """

    def __init__(self, host, push_port, pull_port, secure=False):
        # Host address to bind sockets to
        self._host = host
        # Send digital signed data
        self._secure = secure
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # ZMQ settings
        self._ctx = CloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels using TCP sockets,
        respectively used to send tasks and to retrieve results back"""
        self._pull_socket.connect(f'tcp://{self._host}:{self._pull_port}')
        self._push_socket.connect(f'tcp://{self._host}:{self._push_port}')

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        self._pull_socket.disconnect(f'tcp://{self._host}:{self._pull_port}')
        self._push_socket.disconnect(f'tcp://{self._host}:{self._push_port}')

    def close(self):
        """Close sockets connected to workers, destroy zmq cotext"""
        self._pull_socket.close()
        self._push_socket.close()
        self._ctx.destroy()

    def send(self, data, flags=0):
        """Send data through the PUSH socket, if a secure flag is set it sign it before sending"""
        if self._secure is False:
            self._push_socket.send_data(data, flags)
        else:
            self._push_socket.send_signed(data, flags)

    def recv(self, flags=0):
        """Receive data from the PULL socket, if a secure flag is set it checks for integrity of the
        received data"""
        if self._secure is False:
            return self._pull_socket.recv_data(flags)
        return self._pull_socket.socket.recv_signed(flags)


class UnixConnection(Connection):

    """
    Connection class derived from Connection, set up two communication channels, a PUSH and a PULL
    channel using two synchronous UNIX sockets. Each socket is a subclass of zmq sockets given the
    capability to handle cloudpickled data
    """

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels using UNIX sockets,
        respectively used to send tasks and to retrieve results back"""
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        self._pull_socket.disconnect(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.disconnect(f'ipc://{self._host}-{self._push_port}')


class ConnectionFactory:

    """Abstract Factory class, expose static methods to create the correct class"""

    @staticmethod
    def make_server(host, push_port, pull_port, secure, unix_socket):
        """Create and return a Server class, if unix_socket flag is set to true, a UnixServer class
        is istantiated and returned instead."""
        if unix_socket is False:
            return Server(host, push_port, pull_port, secure)
        return UnixServer(host, push_port, pull_port, secure)

    @staticmethod
    def make_client(host, push_port, pull_port, secure, unix_socket):
        """Create and return a Connection class, if unix_socket flag is set to true, a
        UnixConnection class is istantiated and returned instead."""
        if unix_socket is False:
            return Connection(host, push_port, pull_port, secure)
        return UnixConnection(host, push_port, pull_port, secure)
