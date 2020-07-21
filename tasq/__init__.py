from urllib.parse import urlparse
from tasq.queue import TasqQueue, TasqMultiQueue
from tasq.remote.connection import ZMQBackendConnection, BackendConnection
from tasq.worker.jobqueue import JobQueue
from tasq.worker.executor import ProcessQueueExecutor
from tasq.remote.client import Client
from tasq.remote.backend import RedisStoreBackend, RedisBackend
from tasq.remote.runner import Runners
from tasq.worker.actors import ClientWorker
from tasq.actors.routers import RoundRobinRouter, actor_pool

__author__ = "Andrea Giacomo Baldan"
__license__ = "GPL v3"
__version__ = "1.2.4"
__maintainer__ = "Andrea Giacomo Baldan"
__email__ = "a.g.baldan@gmail.com"
__status__ = "Development"


_backends = {
    "redis": BackendConnection,
    "amqp": BackendConnection,
    "unix": ZMQBackendConnection,
    "zmq": ZMQBackendConnection,
    "tcp": ZMQBackendConnection,
    "ipc": ZMQBackendConnection,
}


def queue(backend="zmq://localhost:9000", store=None, signkey=None):
    """
    Create a TasqQueue instance.
    The formats accepted for the backends are:

    - redis://localhost:6379/0?name=redis-queue
    - amqp://localhost:5672?name=amqp-queue
    - zmq://localhost:9000?pull_port=9010
    - tcp://localhost:5555

    For the store part currently only Redis is supported as a backend:

    - redis://localhost:6379/1?name=results-store

    Attributes
    ----------
    :type backend: str or u'zmq://localhost:9000'
    :param backend: An URL to connect to for the backend service

    :type store: str or None
    :param store: An URL to connect to for the results store service

    :type signkey: str or None
    :param signkey: A string representing a shared key, sign data with a shared
                    key
    """
    if isinstance(backend, str):
        url_parsed = urlparse(backend)
        scheme = url_parsed.scheme or "zmq"
        assert scheme in _backends, f"Unsupported {scheme} as backend"
        _backend = _backends[scheme].from_url(backend, signkey)
        client = Client(_backend)
    elif isinstance(backend, RedisBackend):
        client = Client(BackendConnection(backend))
    if store:
        urlstore = urlparse(store)
        assert urlstore.scheme in {
            "redis"
        }, f"Unknown {urlstore.scheme} as store"
        db = int(urlstore.path.split("/")[-1]) if urlstore.query else 0
        store = RedisStoreBackend(urlstore.hostname, urlstore.port, db)
    return TasqQueue(client, store)


def multi_queue(urls, router_class=RoundRobinRouter, signkey=None):
    assert all(
        isinstance(url, tuple) or isinstance(url, str) for url in urls
    ), "urls argument must be a tuple (host, push_port, pull_port) or a string"
    backends = []
    for url in urls:
        if isinstance(url, tuple):
            host, push_port, pull_port = url
            backends.append(
                Client(
                    ZMQBackendConnection(
                        host, push_port, pull_port, signkey=signkey
                    )
                )
            )
        elif isinstance(url, str):
            url_parsed = urlparse(url)
            scheme = url_parsed.scheme or "zmq"
            assert scheme in (
                "zmq",
                "tcp",
                "unix",
            ), f"Unsupported {scheme} as backend"
            backends.append(Client(_backends[scheme].from_url(url, signkey)))
    return TasqMultiQueue(
        backends,
        lambda: actor_pool(
            num_workers=len(backends),
            actor_class=ClientWorker,
            router_class=router_class,
            clients=backends,
        ),
    )
