"""
tasq.remote.connectio.py
~~~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to define connections using zmq sockets.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from abc import ABCMeta, abstractmethod

import zmq
from .sockets import AsyncCloudPickleContext, CloudPickleContext
from .sockets import RedisBrokerSocket


class Server(metaclass=ABCMeta):

    """
    Connection class, set up two communication channels, a PUSH one using a
    synchronous socket and a PULL one using an asynchronous socket. Each socket
    is a subclass of zmq sockets given the capability to handle cloudpickled
    data
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
        self._ctx = None
        self._push_socket = None
        self._pull_socket = None
        self._make_sockets()

    @abstractmethod
    def _make_sockets(self):
        pass

    @property
    def push_socket(self):
        return self._push_socket

    @property
    def pull_socket(self):
        return self._pull_socket

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor"""
        self._pull_socket.bind(f'tcp://{self._host}:{self._pull_port}')
        self._push_socket.bind(f'tcp://{self._host}:{self._push_port}')

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx.destroy()


class MixedServer(Server):

    """
    Connection class, set up two communication channels, a PUSH one using a
    synchronous socket and a PULL one using an asynchronous socket. Each socket
    is a subclass of zmq sockets given the capability to handle cloudpickled
    data
    """

    def _make_sockets(self):
        self._ctx = {'sync': CloudPickleContext(), 'async': AsyncCloudPickleContext()}
        self._push_socket = self._ctx['sync'].socket(zmq.PUSH)
        self._pull_socket = self._ctx['async'].socket(zmq.PULL)

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx['sync'].destroy()
        self._ctx['async'].destroy()

    def send(self, data, flags=0):
        """Send data through the PUSH socket, if a secure flag is set it sign
        it before sending"""
        if self._secure is False:
            self._push_socket.send_data(data, flags)
        else:
            self._push_socket.send_signed(data, flags)

    async def recv(self, unpickle=True, flags=0):
        """Asynchronous receive data from the PULL socket, if a secure flag is
        set it checks for integrity of the received data"""
        if self._secure is False:
            return await self._pull_socket.recv_data(unpickle, flags)
        return await self._pull_socket.socket.recv_signed(unpickle, flags)


class AsyncServer(Server):

    def _make_sockets(self):
        self._ctx = AsyncCloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)

    async def send(self, data, unpickle=True, flags=0):
        """Send data through the PUSH socket, if a secure flag is set it sign
        it before sending"""
        if self._secure is False:
            await self._push_socket.send_data(data, unpickle, flags)
        else:
            await self._push_socket.send_signed(data, unpickle, flags)

    async def recv(self, unpickle=True, flags=0):
        """Asynchronous receive data from the PULL socket, if a secure flag is
        set it checks for integrity of the received data"""
        if self._secure is False:
            return await self._pull_socket.recv_data(unpickle, flags)
        return await self._pull_socket.socket.recv_signed(unpickle, flags)


class MixedUnixServer(MixedServer):

    """
    Connection class derived from MixedServer, which uses UNIX sockets
    instead of TCP sockets, pull and push port are used to define the name of
    the file descriptor instead of the address.

    e.g.
    PUSH unix socket: /tmp/master-9091
    PULL unix socket: /tmp/master-9092
    """

    def bind(self):
        """
        Binds PUSH and PULL channel sockets to the respective address-port
        pairs defined in the constructor, being used UNIX sockets, push and
        pull ports are used to define file descriptor on the filesystem.
        """
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')


class AsyncUnixServer(AsyncServer):

    """
    Connection class derived from MixedServer, which uses UNIX sockets
    instead of TCP sockets, pull and push port are used to define the name of
    the file descriptor instead of the address.

    e.g.
    PUSH unix socket: /tmp/master-9091
    PULL unix socket: /tmp/master-9092
    """

    def bind(self):
        """
        Binds PUSH and PULL channel sockets to the respective address-port
        pairs defined in the constructor, being used UNIX sockets, push and
        pull ports are used to define file descriptor on the filesystem.
        """
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')


class Connection:

    """
    Connection class, set up two communication channels, a PUSH and a PULL
    channel using two synchronous TCP sockets. Each socket is a subclass of zmq
    sockets given the capability to handle cloudpickled data
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
        """
        Connect to the remote workers, setting up PUSH and PULL channels
        using TCP sockets, respectively used to send tasks and to retrieve
        results back
        """
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
        """Send data through the PUSH socket, if a secure flag is set it sign
        it before sending"""
        if self._secure is False:
            self._push_socket.send_data(data, flags)
        else:
            self._push_socket.send_signed(data, flags)

    def recv(self, unpickle=True, flags=0):
        """Receive data from the PULL socket, if a secure flag is set it checks
        for integrity of the received data"""
        if self._secure is False:
            return self._pull_socket.recv_data(unpickle, flags)
        return self._pull_socket.socket.recv_signed(unpickle, flags)


class UnixConnection(Connection):

    """
    Connection class derived from Connection, set up two communication
    channels, a PUSH and a PULL channel using two synchronous UNIX sockets.
    Each socket is a subclass of zmq sockets given the capability to handle
    cloudpickled data
    """

    def connect(self):
        """
        Connect to the remote workers, setting up PUSH and PULL channels
        using UNIX sockets, respectively used to send tasks and to retrieve
        results back
        """
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        self._pull_socket.disconnect(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.disconnect(f'ipc://{self._host}-{self._push_port}')


class RedisConnection:

    def __init__(self, host, port, db, name, namespace='queue', secure=False):
        self._rb = RedisBrokerSocket(host, port, db, name, namespace)
        self._secure = secure

    def send(self, data):
        """Send data through the PUSH socket, if a secure flag is set it sign
        it before sending"""
        if self._secure is False:
            self._rb.send_data(data)
        else:
            self._rb.send_signed(data)

    def send_result(self, result):
        if self._secure is False:
            self._rb.send_result_data(result)
        else:
            self._rb.send_result_signed(result)

    def recv(self, timeout=None, unpickle=True):
        """Receive data from the PULL socket, if a secure flag is set it checks
        for integrity of the received data"""
        if self._secure is False:
            return self._rb.recv_data(timeout, unpickle)
        return self._rb.recv_signed(timeout, unpickle)

    def recv_result(self, timeout=None, unpickle=True):
        if self._secure is False:
            return self._rb.recv_result_data(timeout, unpickle)
        return self._rb.recv_result_signed(timeout, unpickle)


class ConnectionFactory:

    """Abstract Factory class, expose static methods to create the correct
    class"""

    @staticmethod
    def make_server(host, push_port, pull_port, secure, unix_socket):
        """Create and return a MixedServer class, if unix_socket flag is set to
        true, a MixedUnixServer class is istantiated and returned instead."""
        if unix_socket is False:
            return MixedServer(host, push_port, pull_port, secure)
        return MixedUnixServer(host, push_port, pull_port, secure)

    @staticmethod
    def make_asyncserver(host, push_port, pull_port, secure, unix_socket):
        """Create and return a MixedServer class, if unix_socket flag is set to
        true, a MixedUnixServer class is istantiated and returned instead."""
        if unix_socket is False:
            return AsyncServer(host, push_port, pull_port, secure)
        return AsyncUnixServer(host, push_port, pull_port, secure)

    @staticmethod
    def make_client(host, push_port, pull_port, secure, unix_socket):
        """Create and return a Connection class, if unix_socket flag is set to
        true, a UnixConnection class is istantiated and returned instead."""
        if unix_socket is False:
            return Connection(host, push_port, pull_port, secure)
        return UnixConnection(host, push_port, pull_port, secure)

    @staticmethod
    def make_redis_client(host, port, db, name, namespace='queue', secure=False):
        return RedisConnection(host, port, db, name, namespace, secure=secure)
