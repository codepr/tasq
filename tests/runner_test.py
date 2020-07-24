import queue
import unittest
import threading
from unittest.mock import patch
from concurrent.futures import Future
from tasq.remote.runner import Runner, ZMQRunner
from tasq.job import Job
from .backend_test import FakeRedisClient, FakeAMQPClient


class FakeWorker:
    def route(self, job):
        fut = Future()
        fut.set_result(job.execute())
        return fut


class FakeBackend:
    def __init__(self, q):
        self.queue = q
        self.results = {}

    def send_result(self, result):
        self.results[result.name] = result

    def recv(self, timeout, unpickle):
        return self.queue.get()


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.q = queue.Queue()
        self.backend = FakeBackend(self.q)
        self.r = Runner(self.backend, lambda: FakeWorker())
        threading.Thread(target=self.r.start, daemon=True).start()

    def test_runner_running(self):
        q = queue.Queue()
        q.put(Job("test", lambda x: x + 1, 2))
        self.assertEqual(self.backend.results, {})

    def test_runner_factory(self):
        from tasq.remote.runner import runner_factory

        with patch("tasq.remote.runner.ZMQBackend") as zmqmock, patch(
            "tasq.remote.runner.asyncio.get_event_loop"
        ) as loopmock, patch("redis.StrictRedis") as rmock, patch(
            "pika.BlockingConnection"
        ) as pmock, patch(
            "tasq.remote.runner.worker.build_jobqueue"
        ) as jq:
            jq.return_value = None
            pmock.return_value = FakeAMQPClient()
            rmock.return_value = FakeRedisClient()
            loopmock.return_value = None
            zmqmock.return_value = None
            zmqrunner = runner_factory.create(
                "ZMQ_ACTOR_RUNNER", host="localhost", channel=(20000, 20001)
            )
            zmqqrunner = runner_factory.create(
                "ZMQ_QUEUE_RUNNER", host="localhost", channel=(20000, 20001)
            )
            redis_actor_runner = runner_factory.create(
                "REDIS_ACTOR_RUNNER",
                host="1.2.3.4",
                port=6333,
                db=1,
                name="test-queue",
            )
            redis_queue_runner = runner_factory.create(
                "REDIS_QUEUE_RUNNER",
                host="1.2.3.4",
                port=6333,
                db=1,
                name="test-queue",
            )
            rabbitmq_queue_runner = runner_factory.create(
                "AMQP_QUEUE_RUNNER",
                host="1.2.3.4",
                port=6333,
                role="sender",
                name="test-queue",
            )
            rabbitmq_actor_runner = runner_factory.create(
                "AMQP_ACTOR_RUNNER",
                host="1.2.3.4",
                port=6333,
                role="sender",
                name="test-queue",
            )
            self.assertTrue(isinstance(zmqrunner, ZMQRunner))
            self.assertTrue(isinstance(zmqqrunner, ZMQRunner))
            self.assertTrue(isinstance(redis_actor_runner, Runner))
            self.assertTrue(isinstance(redis_queue_runner, Runner))
            self.assertTrue(isinstance(rabbitmq_actor_runner, Runner))
            self.assertTrue(isinstance(rabbitmq_queue_runner, Runner))
