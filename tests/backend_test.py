import time
import queue
import asyncio
import unittest
import threading
import collections
from unittest.mock import patch
from tasq.remote.backend import ZMQBackend, RedisBackend, RabbitMQBackend


class FakeSocket:
    def __init__(self):
        self.bind_url = u""
        self.dc_url = u""
        self.data_sent = None

    async def send_data(self, data, flags, signkey):
        self.data_sent = (data, flags, signkey)

    async def recv_data(self, unpickle, flags, signkey):
        return self.data_sent

    def bind(self, url):
        self.bind_url = url

    def connect(self, url):
        self.bind_url = url

    def disconnect(self, url):
        self.dc_url = url

    def close(self):
        pass


class FakeRedisPool:
    def disconnect(self):
        pass


class FakeRedisClient:
    def __init__(self):
        self.queues = collections.defaultdict(list)
        self.connection_pool = FakeRedisPool()

    def dbsize(self):
        return 0

    def llen(self, queue_name):
        return len(self.queues[queue_name])

    def lpush(self, queue_name, item):
        self.queues[queue_name].append(item)

    def brpop(self, queue_name, timeout=0):
        if timeout and not self.queues[queue_name]:
            time.sleep(timeout)
            return None
        return (
            None,
            self.queues[queue_name],
        )

    def rpop(self, queue_name):
        return self.brpop(queue_name)

    def brpoplpush(self, queue_name, queue_name_alt, timeout=0):
        if timeout and not self.queues[queue_name]:
            time.sleep(timeout)
            return None
        item = self.queues[queue_name]
        self.queues[queue_name_alt].append(item)
        return item

    def rpoplpush(self, queue_name, queue_name_alt):
        return self.brpoplpush(queue_name, queue_name_alt)

    def lrange(self, queue_name, start, end):
        return self.queues[queue_name][start:end]


class AMQPMethod:
    def __init__(self):
        self.delivery_tag = None


class FakeAMQPChannel:
    def __init__(self):
        self.run = threading.Event()
        self.queues = collections.defaultdict(queue.Queue)
        self.consume_queue = None
        self.on_message = None

    def basic_publish(self, exchange, queue_name, item):
        self.queues[queue_name].put(item)

    def basic_consume(self, queue, on_message_callback):
        self.on_message = on_message_callback

    def basic_ack(self, delivery_tag):
        pass

    def basic_qos(self, prefetch_count):
        pass

    def queue_declare(self, queue, durable=True):
        self.consume_queue = queue

    def start_consuming(self):
        while not self.run.is_set():
            item = self.queues[self.consume_queue].get()
            if not item:
                break
            self.on_message(self, AMQPMethod(), None, item)

    def stop(self):
        self.queues[self.consume_queue].put(None)
        self.run.set()


_amqp_channel = FakeAMQPChannel()


class FakeAMQPClient:
    def channel(self):
        return _amqp_channel


class TestZMQBackend(unittest.TestCase):
    def test_init_zmqbackend(self):
        with patch(
            "tasq.remote.backend.AsyncCloudPickleContext.socket"
        ) as mock:
            mock.side_effect = lambda _: FakeSocket()
            backend = ZMQBackend("localhost", 10000, 10001)
            backend.bind()
            self.assertEqual(
                backend._pull_socket.bind_url, "tcp://localhost:10001"
            )
            self.assertEqual(
                backend._push_socket.bind_url, "tcp://localhost:10000"
            )
            backend.stop()

    def test_send_zmqbackend(self):
        fake_socket = FakeSocket()
        with patch(
            "tasq.remote.backend.AsyncCloudPickleContext.socket"
        ) as mock:
            mock.return_value = fake_socket
            backend = ZMQBackend("localhost", 10000, 10001)
            backend.bind()
            asyncio.run(backend.send("hello"))
            self.assertEqual(fake_socket.data_sent, ("hello", 0, None))

    def test_recv_zmqbackend(self):
        fake_socket = FakeSocket()
        with patch(
            "tasq.remote.backend.AsyncCloudPickleContext.socket"
        ) as mock:
            mock.return_value = fake_socket
            backend = ZMQBackend("localhost", 10000, 10001)
            backend.bind()
            asyncio.run(backend.send("hello"))
            self.assertEqual(fake_socket.data_sent, ("hello", 0, None))
            payload = asyncio.run(backend.recv())
            self.assertEqual(payload, ("hello", 0, None))


class TestRedisBackend(unittest.TestCase):
    def test_redis_put_job(self):
        backend = RedisBackend(lambda: FakeRedisClient(), name="test-queue")
        backend.put_job("test-job")
        self.assertEqual(backend.get_next_job(), ["test-job"])
        backend.close()

    def test_redis_put_result(self):
        backend = RedisBackend(lambda: FakeRedisClient(), name="test-queue")
        backend.put_result("test-job-result")
        self.assertEqual(backend.get_available_result(), ["test-job-result"])
        backend.close()

    def test_redis_get_next_job(self):
        backend = RedisBackend(lambda: FakeRedisClient(), name="test-queue")
        backend.put_job("test-job")
        self.assertEqual(backend.get_next_job(), ["test-job"])
        backend.close()

    def test_redis_get_pending_jobs(self):
        backend = RedisBackend(lambda: FakeRedisClient(), name="test-queue")
        backend.put_job("test-job")
        self.assertEqual(backend.get_pending_jobs(), ([], []))
        backend.close()


class TestRabbitMQBackend(unittest.TestCase):
    def test_amqp_put_job(self):
        backend = RabbitMQBackend(
            lambda: FakeAMQPClient(), role="receiver", name="test-queue"
        )
        backend.put_job("test-job")
        self.assertEqual(backend.get_next_job(), "test-job")
        backend.stop()
