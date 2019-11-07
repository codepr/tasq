"""
tasq.remote.connectio.py
~~~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to define connections using zmq sockets.
"""
from abc import ABCMeta, abstractmethod

import zmq
from .backends.redis import RedisBackend
from .backends.rabbitmq import RabbitMQBackend
from .sockets import AsyncCloudPickleContext, CloudPickleContext, BackendSocket


class Server(metaclass=ABCMeta):

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

    @property
    def push_socket(self):
        return self._push_socket

    @property
    def pull_socket(self):
        return self._pull_socket

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor
        """
        self._pull_socket.bind(f'tcp://{self._host}:{self._pull_port}')
        self._push_socket.bind(f'tcp://{self._host}:{self._push_port}')

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx.destroy()


class MixedServer(Server):

    """Connection class, set up two communication channels, a PUSH one using a
    synchronous socket and a PULL one using an asynchronous socket. Each socket
    is a subclass of zmq sockets given the capability to handle cloudpickled
    data
    """

    def _make_sockets(self):
        self._ctx = {'sync': CloudPickleContext(),
                     'async': AsyncCloudPickleContext()}
        self._push_socket = self._ctx['sync'].socket(zmq.PUSH)
        self._pull_socket = self._ctx['async'].socket(zmq.PULL)
        # ZMQ poller settings for async recv
        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._pull_socket, zmq.POLLIN)

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx['sync'].destroy()
        self._ctx['async'].destroy()

    def send(self, data, flags=0):
        """Send data through the PUSH socket, if a signkey flag is set it sign
        it before sending
        """
        self._push_socket.send_data(data, flags, self._signkey)

    async def poll(self):
        events = await self._poller.poll()
        if self._pull_socket in dict(events):
            return dict(events)
        return None

    async def recv(self, unpickle=True, flags=0):
        """Asynchronous receive data from the PULL socket, if a signkey flag is
        set it checks for integrity of the received data
        """
        return await self._pull_socket.recv_data(unpickle, flags, self._signkey)


class AsyncServer(Server):

    def _make_sockets(self):
        self._ctx = AsyncCloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)

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


class MixedUnixServer(MixedServer):

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
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')


class AsyncUnixServer(AsyncServer):

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
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')


class Connection:

    """Connection class, set up two communication channels, a PUSH and a PULL
    channel using two synchronous TCP sockets. Each socket is a subclass of zmq
    sockets given the capability to handle cloudpickled data
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
        self._ctx = CloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels
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
        """Send data through the PUSH socket, if a signkey flag is set it sign
        it before sending
        """
        self._push_socket.send_data(data, flags, self._signkey)

    def recv(self, unpickle=True, flags=0):
        """Receive data from the PULL socket, if a signkey flag is set it
        checks for integrity of the received data
        """
        return self._pull_socket.recv_data(unpickle, flags, self._signkey)


class UnixConnection(Connection):

    """Connection class derived from Connection, set up two communication
    channels, a PUSH and a PULL channel using two synchronous UNIX sockets.
    Each socket is a subclass of zmq sockets given the capability to handle
    cloudpickled data
    """

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels
        using UNIX sockets, respectively used to send tasks and to retrieve
        results back
        """
        self._pull_socket.bind(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.bind(f'ipc://{self._host}-{self._push_port}')

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        self._pull_socket.disconnect(f'ipc://{self._host}-{self._pull_port}')
        self._push_socket.disconnect(f'ipc://{self._host}-{self._push_port}')


class BackendConnection:

    def __init__(self, backend, signkey=None):
        self._backend = backend
        self._signkey = signkey

    def send(self, data):
        """Send data through the PUSH socket, if a signkey flag is set it sign
        it before sending
        """
        self._backend.send_data(data, self._signkey)

    def send_result(self, result):
        self._backend.send_result_data(result, self._signkey)

    def recv(self, timeout=None, unpickle=True):
        """Receive data from the PULL socket, if a signkey flag is set it checks
        for integrity of the received data
        """
        return self._backend.recv_data(timeout, unpickle, self._signkey)

    def recv_result(self, timeout=None, unpickle=True):
        return self._backend.recv_result_data(timeout, unpickle, self._signkey)

    def get_pending_jobs(self):
        return self._backend.get_pending_jobs()

    def close(self):
        self._backend.close()


class ConnectionFactory:

    """Poor Factory class, expose static methods to create the correct
    class, nothing more
    """

    @staticmethod
    def make_server(host, push_port, pull_port, signkey, unix_socket):
        """Create and return a MixedServer class, if unix_socket flag is set to
        true, a MixedUnixServer class is istantiated and returned instead.
        """
        if unix_socket is False:
            return MixedServer(host, push_port, pull_port, signkey)
        return MixedUnixServer(host, push_port, pull_port, signkey)

    @staticmethod
    def make_asyncserver(host, push_port, pull_port, signkey, unix_socket):
        """Create and return a MixedServer class, if unix_socket flag is set to
        true, a MixedUnixServer class is istantiated and returned instead.
        """
        if unix_socket is False:
            return AsyncServer(host, push_port, pull_port, signkey)
        return AsyncUnixServer(host, push_port, pull_port, signkey)

    @staticmethod
    def make_client(host, push_port, pull_port, signkey, unix_socket):
        """Create and return a Connection class, if unix_socket flag is set to
        true, a UnixConnection class is istantiated and returned instead.
        """
        if unix_socket is False:
            return Connection(host, push_port, pull_port, signkey)
        return UnixConnection(host, push_port, pull_port, signkey)

    @staticmethod
    def make_redis_client(host, port, db, name,
                          namespace='queue', signkey=None):
        return BackendConnection(
            BackendSocket(RedisBackend(host, port, db, name, namespace)),
            signkey=signkey
        )

    @staticmethod
    def make_rabbitmq_client(host, port, role, name,
                             namespace='queue', signkey=None):
        return BackendConnection(
            BackendSocket(RabbitMQBackend(host, port, role, name, namespace)),
            signkey=signkey
        )
