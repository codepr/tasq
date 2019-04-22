"""
tasq.remote.backends.redis.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Redis backend connection implementation, provides functions and classes to
handle a redis queue in a more convenient way for the system.
"""

import json
try:
    import redis
except ImportError:
    print("You need to install redis python driver to use redis backend")

from tasq.logger import get_logger


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

        def __init__(self, name, host, port, db, namespace='queue'):
            """The default connection parameters are: host='localhost',
            port=6379, db=0
            """
            self._db = redis.StrictRedis(host=host, port=port, db=db)

            try:
                _ = self._db.dbsize()
            except ConnectionError as e:
                self.log.warning('Connection to DB failed with error %s.', e)

            self._queue_name = f'{namespace}:{name}'
            self._work_queue_name = f'{namespace}:{name}:work'

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
                item = self._db.brpoplpush(self._queue_name,
                                           self._work_queue_name, timeout)
            else:
                item = self._db.rpoplpush(self._queue_name,
                                          self._work_queue_name)

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

    def __init__(self, host, port, db, name, namespace='queue'):

        self._rq = self.RedisQueue(name, host, port, db, namespace)
        self._rq_res = self.RedisQueue(f'{name}:result', host,
                                       port, db, namespace)

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

    def close(self):
        self._rq.close()
        self._rq_res.close()


class RedisStore:

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
                self.log.warning('Connection to DB failed with error %s.', e)

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
                return json.loads('{}')
            return json.loads(data)

        def read_all(self):
            """Read all data contained into the redis instance.

            :rtype: dict
            :return: A dictionary that contains all data.
            """
            items = self._db.keys()
            return {k.decode('utf8'): self.read(k) for k in items}

    def __init__(self, host, port, db):

        self._rd = self.RedisDict(host, port, db)

    def put_result(self, job_result):
        self._rd.write(job_result.name, job_result.value or job_result.exc)

    def get_result(self, key):
        return self._rd.read(key)
