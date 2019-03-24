"""
"""

import json
try:
    import redis
except ImportError:
    print("You need to install redis python driver to use redis backend")

from tasq.logger import get_logger


class RedisBroker:

    class RedisQueue:

        """Simple Queue with Redis Backend"""

        log = get_logger(__name__)

        def __init__(self, name, host, port, db, namespace='queue'):
            """The default connection parameters are: host='localhost', port=6379, db=0"""

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

        def get(self, block=True, timeout=None):
            """Remove and return an item from the queue.

            If optional args block is true and timeout is None (the default), block
            if necessary until an item is available."""
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

        def get_and_push(self, block=True, timeout=None):
            """Get the tail of the queue, push into the head of the work queue
            and return it in a single atomically transaction"""
            if block:
                item = self._db.brpoplpush(self._queue_name, self._work_queue_name)
            else:
                item = self._db.rpoplpush(self._queue_name, self._work_queue_name)

            return item

        def list_working_items(self):
            """Retrieve all items in the work_queue using LRANGE command"""
            return self._db.lrange(self._work_queue_name, 0, -1)

    def __init__(self, host, port, db, name, namespace='queue'):

        self._rq = self.RedisQueue(name, host, port, db, namespace)
        self._rq_res = self.RedisQueue(f'{name}:result', host, port, db, namespace)

    def put_job(self, serialized_job):
        self._rq.put(serialized_job)

    def put_result(self, result):
        self._rq_res.put(result)

    def get_next_job(self, timeout=None):
        return self._rq.get_and_push(True, timeout)

    def get_available_result(self, timeout=None):
        return self._rq_res.get(True, timeout)

    def get_working_jobs(self):
        return self._rq.list_working_items()


class RedisBackend:

    class RedisDict:

        """
        Database class to manage redis connection and read/write operations,
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

            :return: The dictionary containing data associated to the key
            """
            data = self._db.get(key)
            if data is None:
                return json.loads('{}')
            return json.loads(data)

        def read_all(self):
            """Read all data contained into the redis instance.

            :return: A list of dictionaries that contains all data.
            """
            devices = self._db.keys()
            return {k.decode('utf8'): self.read(k) for k in devices if k not in self.excludes}

    def __init__(self, host, port, db):

        self._rd = self.RedisDict(host, port, db)

    def put_result(self, job_result):
        self._rd.write(job_result.name, job_result.value or job_result.exc)

    def get_result(self, key):
        return self._rd.read(key)
