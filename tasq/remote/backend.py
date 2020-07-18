"""
tasq.remote.backend.py
~~~~~~~~~~~~~~~~~~~~~~

This module contains classes to create backends.
"""

import json
import queue
import threading

try:
    import redis
except ImportError:
    print("You need to install redis python driver to use redis backend")
try:
    import zmq
except ImportError:
    print("You need to install zmq python driver to use ZMQ backend")
try:
    import pika
except ImportError:
    print("You need to install pika to use rabbitmq backend")


from .sockets import AsyncCloudPickleContext
from tasq.logger import get_logger


class ZMQBackend:

    """Connection class, set up two communication channels, a PUSH one using a
    synchronous socket and a PULL one using an asynchronous socket. Each socket
    is a subclass of zmq sockets given the capability to handle cloudpickled
    data
    """

    def __init__(self, host, push_port, pull_port, signkey=None, unix=False):
        # Host address to bind sockets to
        self._host = host
        # Send digital signed data
        self._signkey = signkey
        # Port for pull side (ingoing) of the communication channel
        self._pull_port = pull_port
        # Port for push side (outgoing) of the communication channel
        self._push_port = push_port
        # ZMQ settings
        self._ctx = AsyncCloudPickleContext()
        self._push_socket = self._ctx.socket(zmq.PUSH)
        self._pull_socket = self._ctx.socket(zmq.PULL)
        # ZMQ poller settings for async recv
        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._pull_socket, zmq.POLLIN)
        self._unix = unix

    def bind(self):
        """Binds PUSH and PULL channel sockets to the respective address:port
        pairs defined in the constructor
        """
        protocol = "tcp" if not self._unix else "ipc"
        self._pull_socket.bind(f"{protocol}://{self._host}:{self._pull_port}")
        self._push_socket.bind(f"{protocol}://{self._host}:{self._push_port}")

    def stop(self):
        # Close connected sockets
        self._pull_socket.close()
        self._push_socket.close()
        # Destroy the contexts
        self._ctx.destroy()

    async def poll(self):
        events = await self._poller.poll()
        if self._pull_socket in dict(events):
            return dict(events)
        return None

    async def send(self, data, flags=0):
        """Send data through the PUSH socket, if a signkey flag is set it sign
        it before sending
        """
        await self._push_socket.send_data(data, flags, self._signkey)

    async def recv(self, unpickle=True, flags=0):
        """Asynchronous receive data from the PULL socket, if a signkey flag is
        set it checks for integrity of the received data
        """
        return await self._pull_socket.recv_data(
            unpickle, flags, self._signkey
        )


class RedisBackend:

    """Redis backend implementation, it' s composed by two RedisQueue which is
    an inner class object. The main queue is used to enqueue incoming jobs from
    clients, the second queue is used as a result queue, every job that is
    executed with a result will be enqueued there, ready to be polled by third
    consumers.

    Args
    ----
    :type host: str
    :param host: The host where the Redis instance is running

    :type port: int
    :param port: The listening port of the Redis instance, usually 6379

    :type db: int
    :param db: The Redis DB where we instantiate our queues

    :type name: str
    :param name: The name of queue, for the result a ":result" will be
                 appended

    :type namespace: str or 'queue'
    :param namespace: The namespace to be used for the queues, usefull to
                      categorize different batches of jobs.

    """

    class RedisQueue:

        """Simple blocking queue with Redis as backend. It deploy two different
        queues, the main queue used to store the incoming jobs, the second one
        is a sort of 'busy-queue', every time a job is polled by a Supervisor
        to be executed, it will also be enqueued in that queue, to be removed
        only after the complete execution with a result. This way it gives an
        idea of the remaining jobs and a sort of recovery mechanism in case of
        crash during execution.
        """

        log = get_logger(__name__)

        def __init__(self, name, host, port, db, namespace="queue"):
            """The default connection parameters are: host='localhost',
            port=6379, db=0
            """
            self._db = redis.StrictRedis(host=host, port=port, db=db)

            try:
                _ = self._db.dbsize()
            except ConnectionError as e:
                self.log.warning("Connection to DB failed with error %s.", e)

            self._queue_name = f"{namespace}:{name}"
            self._work_queue_name = f"{namespace}:{name}:work"

        def qsize(self):
            """Return the approximate size of the queue."""
            return self._db.llen(self._queue_name)

        def empty(self):
            """Return True if the queue is empty, False otherwise."""
            return self.qsize() == 0

        def put(self, item):
            """Put item into the queue."""
            self._db.lpush(self._queue_name, item)

        def get(self, block=True, timeout=None) -> bytes:
            """Remove and return an item from the queue.

            If optional args block is true and timeout is None (the default),
            block if necessary until an item is available.

            Args
            ----
            :type block: bool or True
            :param block: Boolean flag for block the call or return
                          immediatly regardless of the result

            :type timeout: int or None
            :param timeout: Time to wait for an item to be available otherwise
                            return None. None as value means wait forever.

            :rtype: bytes
            :return: A bytes object containing the last item in queue

            """
            if block:
                item = self._db.brpop(self._queue_name, timeout=timeout)
            else:
                item = self._db.rpop(self._queue_name)

            if item:
                item = item[1]
            return item

        def get_nowait(self):
            """Equivalent to get(False)."""
            return self.get(False)

        def get_and_push(self, block=True, timeout=None) -> bytes:
            """Get the tail of the queue, push into the head of the work queue
            and return it in a single atomically transaction.

            Args
            ----
            :type block: bool or True
            :param block: Boolean flag for block the call or return
                          immediatly regardless of the result

            :type timeout: int or None
            :param timeout: Time to wait for an item to be available otherwise
                            return None. None as value means wait forever.

            :rtype: bytes
            :return: A bytes object containing the last item in queue

            """
            if block:
                item = self._db.brpoplpush(
                    self._queue_name, self._work_queue_name, timeout
                )
            else:
                item = self._db.rpoplpush(
                    self._queue_name, self._work_queue_name
                )

            return item

        def list_pending_items(self):
            """Retrieve all items in the main queue using LRANGE command.

            :rtype: list(bytes)
            :return: A list of bytes items
            """
            return self._db.lrange(self._queue_name, 0, -1)

        def list_working_items(self):
            """Retrieve all items in the work_queue using LRANGE command.

            :rtype: list(bytes)
            :return: A list of bytes items
            """
            return self._db.lrange(self._work_queue_name, 0, -1)

        def close(self):
            self._db.connection_pool.disconnect()

    def __init__(self, host, port, db, name, namespace="queue"):

        self._rq = self.RedisQueue(name, host, port, db, namespace)
        self._rq_res = self.RedisQueue(
            f"{name}:result", host, port, db, namespace
        )
        self._host = host
        self._port = port
        self._db = db
        self._namespace = f"{namespace}:{name}"

    def __repr__(self):
        return (
            f"RedisBackend(redis://{self._host}:{self._port}/{self._db}"
            f"?name={self._namespace}"
        )

    def put_job(self, serialized_job):
        self._rq.put(serialized_job)

    def put_result(self, result):
        self._rq_res.put(result)

    def get_next_job(self, timeout=None):
        return self._rq.get_and_push(True, timeout)

    def get_available_result(self, timeout=None):
        return self._rq_res.get(True, timeout)

    def get_pending_jobs(self):
        return self._rq.list_pending_items(), self._rq.list_working_items()

    def stop(self):
        self.close()

    def close(self):
        self._rq.close()
        self._rq_res.close()


class RedisStoreBackend:
    class RedisDict:

        """Database class to manage redis connection and read/write operations,
        implemented as a SingletonArgs metaclass in order to obtain a singleton
        based on the args passed;

        e.g. there can only be one instance of `Db` with host=127.0.0.1,
        port=9999, db=5.
        """

        log = get_logger(__name__)

        def __init__(self, host, port, db):
            # Database
            self._db = redis.StrictRedis(host=host, port=port, db=db)
            try:
                _ = self._db.dbsize()
            except ConnectionError as e:
                self.log.warning("Connection to DB failed with error %s.", e)

        def write(self, key, dic):
            """Write dictionary to redis instance.

            Args:
            -----
            :type key: str
            :param key: The key of the dictionary to write into the DB

            :type dic: dict
            :param dic: The dictionary containing data to be stored into the DB
            """
            json_dic = json.dumps(dic)
            self._db.set(key, json_dic)

        def read(self, key):
            """Read data from redis instance.

            Args:
            -----
            :type key: str
            :param key: The key of the dictionary to read data from DB

            :rtype: dict
            :return: The dictionary containing data associated to the key
            """
            data = self._db.get(key)
            if data is None:
                return json.loads("{}")
            return json.loads(data)

        def read_all(self):
            """Read all data contained into the redis instance.

            :rtype: dict
            :return: A dictionary that contains all data.
            """
            items = self._db.keys()
            return {k.decode("utf8"): self.read(k) for k in items}

    def __init__(self, host, port, db):
        self._rd = self.RedisDict(host, port, db)
        self._host = host
        self._port = port
        self._db = db

    def __repr__(self):
        return f"RedisStoreBackend(redis://{self._host}:{self._port}/{self._db}"

    def put_result(self, job_result):
        self._rd.write(job_result.name, job_result.value or job_result.exc)

    def get_result(self, key):
        return self._rd.read(key)


class RabbitMQBackend:

    """Simple Queue with RabbitMQ Backend"""

    def __init__(self, host, port, role, name, namespace="queue"):
        """The default connection parameters are: host='localhost', port=5672
        """
        self._host, self._port = host, port
        assert role in {"receiver", "sender"}, f"Unknown role {role}"
        self._role = role
        self._queue_name = f"{namespace}:{name}"
        self._result_name = f"{namespace}:{name}:result"
        # Blocking queues
        self._jobs = queue.Queue()
        self._results = queue.Queue()
        threading.Thread(target=self._start, daemon=True).start()

    def _get_channel(self):
        channel = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port)
        ).channel()
        return channel

    def _get_job(self, ch, method, _, body):
        self._jobs.put(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _get_res(self, ch, method, _, body):
        self._results.put(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _start(self):
        channel = self._get_channel()
        channel.basic_qos(prefetch_count=1)
        if self._role == "receiver":
            channel.queue_declare(queue=self._queue_name, durable=True)
            channel.basic_consume(
                queue=self._queue_name, on_message_callback=self._get_job
            )
        else:
            channel.queue_declare(queue=self._result_name, durable=True)
            channel.basic_consume(
                queue=self._result_name, on_message_callback=self._get_res
            )
        channel.start_consuming()

    def __repr__(self):
        return (
            f"RabbitMQBackend(amqp://{self._host}:{self._port}/"
            f"?name={self._name}, role=self._role)"
        )

    def put_job(self, serialized_job):
        channel = self._get_channel()
        channel.basic_publish("", self._queue_name, serialized_job)

    def put_result(self, result):
        channel = self._get_channel()
        channel.basic_publish("", self._result_name, result)

    def get_next_job(self, timeout=None):
        try:
            return self._jobs.get(timeout=timeout)
        except queue.Empty:
            return None

    def get_available_result(self, timeout=None):
        try:
            return self._results.get(timeout=timeout)
        except queue.Empty:
            return None

    def stop(self):
        pass

    def close(self):
        pass
