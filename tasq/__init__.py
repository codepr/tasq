from urllib.parse import urlparse
from tasq.queue import TasqQueue
from tasq.remote.connection import ZMQBackendConnection, BackendConnection
from tasq.worker.jobqueue import JobQueue
from tasq.worker.executor import ProcessQueueExecutor
from tasq.remote.client import Client
from tasq.remote.backend import RedisStoreBackend
from tasq.remote.runner import Runners

__author__ = "Andrea Giacomo Baldan"
__license__ = "GPL v3"
__version__ = "1.2.2"
__maintainer__ = "Andrea Giacomo Baldan"
__email__ = "a.g.baldan@gmail.com"
__status__ = "Development"


_backends = {
    "redis": BackendConnection,
    "amqp": BackendConnection,
    "unix": ZMQBackendConnection,
    "zmq": ZMQBackendConnection,
    "tcp": ZMQBackendConnection,
}


def queue(url="zmq://localhost:9000", store=None, signkey=None):
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

    :type signkey: bool or False
    :param signkey: A boolean flag, sign data with a shared key
    """
    url_parsed = urlparse(url)
    scheme = url_parsed.scheme or "zmq"
    assert scheme in _backends, f"Unsupported {url.scheme} as backend"
    backend = _backends[scheme].from_url(url, signkey)
    client = Client(backend)
    if store:
        urlstore = urlparse(store)
        assert urlstore.scheme in {"redis"}, f"Unknown {scheme} as store"
        db = int(urlstore.path.split("/")[-1]) if url.query else 0
        store = RedisStoreBackend(urlstore.hostname, urlstore.port, db)
    return TasqQueue(client, store)
