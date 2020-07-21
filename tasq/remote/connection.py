"""
tasq.remote.connection.py
~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to define connections using zmq sockets.
"""

import zmq
from urllib.parse import urlparse

try:
    import redis
except ImportError:
    print("You need to install redis python driver to use redis backend")

from .backend import RedisBackend, RabbitMQBackend
from .sockets import CloudPickleContext, BackendSocket
from ..exception import BackendCommunicationErrorException


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

    def __repr__(self):
        protocol = "unix" if self._unix else "zmq"
        return (
            f"ZMQBackendConnection({protocol}://{self._host}:{self._channel})"
        )

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
        try:
            self._push_socket.send_data(data, flags, self._signkey)
        except (zmq.error.ContextTerminated, zmq.error.ZMQError) as e:
            raise BackendCommunicationErrorException(str(e))

    def recv(self, unpickle=True, flags=0):
        """Receive data from the PULL socket, if a signkey flag is set it
        checks for integrity of the received data
        """
        try:
            data = self._pull_socket.recv_data(unpickle, flags, self._signkey)
        except (zmq.error.ContextTerminated, zmq.error.ZMQError) as e:
            raise BackendCommunicationErrorException(str(e))
        else:
            return data

    def recv_result(self, unpickle=True, flags=0):
        return self.recv(unpickle, flags)

    @classmethod
    def from_url(cls, url, signkey=None):
        u = urlparse(url)
        scheme = u.scheme or "zmq"
        assert scheme in ("zmq", "unix", "tcp"), f"Unsupported {scheme}"
        extras = {
            t.split("=")[0]: t.split("=")[1] for t in u.query.split("?") if t
        }
        extras = {k: v for k, v in extras.items() if k == "pull_port"}
        conn_args = {
            "host": u.hostname or "127.0.0.1",
            "push_port": u.port or 9000,
            "pull_port": int(extras.get("pull_port", (u.port or 9000) + 1)),
            "unix": scheme == "unix",
            "signkey": signkey,
        }
        return cls(**conn_args)


class BackendConnection:
    def __init__(self, backend, signkey=None):
        self._backend = BackendSocket(backend)
        self._signkey = signkey

    def __repr__(self):
        return f"BackendConnection({self._backend})"

    def connect(self):
        pass

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

    @classmethod
    def from_url(cls, url, signkey=None):
        u = urlparse(url)
        scheme = u.scheme or "redis"
        assert scheme in ("redis", "amqp"), f"Unsupported {scheme}"
        extraparams = {
            t.split("=")[0]: t.split("=")[1] for t in u.query.split("?") if t
        }
        extraparams = {
            k: v for k, v in extraparams.items() if k in {"name", "db"}
        }
        name = extraparams.get(
            "name", "amqp-queue" if scheme == "amqp" else "redis-queue"
        )
        conn_args = {
            "host": u.hostname or "localhost",
            "port": u.port or 6379,
        }
        if scheme == "redis":
            conn_args["db"] = int(extraparams.get("db", 0))
            backend = RedisBackend(
                lambda: redis.StrictRedis(**conn_args), name=name
            )
        else:
            conn_args["role"] = extraparams.get("role", "sender")
            backend = RabbitMQBackend(**conn_args)
        return cls(backend, signkey)


def connect_redis_backend(
    host, port, db, name, namespace="queue", signkey=None
):
    return BackendConnection(
        RedisBackend(
            lambda: redis.StrictRedis(host, port, db), name, namespace
        ),
        signkey=signkey,
    )


def connect_rabbitmq_backend(
    host, port, role, name, namespace="queue", signkey=None
):
    return BackendConnection(
        RabbitMQBackend(host, port, role, name, namespace), signkey=signkey,
    )
