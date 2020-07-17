"""
tasq.remote.connection.py
~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to define connections using zmq sockets.
"""

import zmq
from .backend import RedisBackend, RabbitMQBackend
from .sockets import CloudPickleContext, BackendSocket


class ZMQBackendConnection:

    """Connection class, set up two communication channels, a PUSH and a PULL
    channel using two synchronous TCP sockets. Each socket is a subclass of zmq
    sockets given the capability to handle cloudpickled data
    """

    def __init__(self, host, push_port, pull_port, unix=False, signkey=None):
        # Host address to bind sockets to
        self._host = host
        # Send digital signed data
        self._signkey = signkey
        # Port for pull side (ingoing) of the communication channel
        # Port for push side (outgoing) of the communication channel
        self._channel = (pull_port, push_port)
        self._unix = unix
        # ZMQ settings
        self._ctx = CloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)

    def connect(self):
        """Connect to the remote workers, setting up PUSH and PULL channels
        using TCP sockets, respectively used to send tasks and to retrieve
        results back
        """
        protocol = "ipc" if self._unix else "tcp"
        pull, push = self._channel
        self._pull_socket.connect(f"{protocol}://{self._host}:{pull}")
        self._push_socket.connect(f"{protocol}://{self._host}:{push}")

    def disconnect(self):
        """Disconnect PUSH and PULL sockets"""
        protocol = "ipc" if self._unix else "tcp"
        pull, push = self._channel
        self._pull_socket.disconnect(f"{protocol}://{self._host}:{pull}")
        self._push_socket.disconnect(f"{protocol}://{self._host}:{push}")

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


class BackendConnection:
    def __init__(self, backend, signkey=None):
        self._backend = BackendSocket(backend)
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

    def stop(self):
        self.close()

    def close(self):
        self._backend.close()


def connect_redis_backend(
    host, port, db, name, namespace="queue", signkey=None
):
    return BackendConnection(
        RedisBackend(host, port, db, name, namespace), signkey=signkey,
    )


def connect_rabbitmq_backend(
    host, port, role, name, namespace="queue", signkey=None
):
    return BackendConnection(
        RabbitMQBackend(host, port, role, name, namespace), signkey=signkey,
    )
