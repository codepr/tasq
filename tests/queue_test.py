import time
import unittest
from tasq import RoundRobinRouter, ClientWorker, actor_pool
from tasq.queue import TasqQueue, TasqMultiQueue
from tasq.job import JobResult


class FakeBackend:
    def __init__(self):
        self.connected = False
        self._pending_jobs = []
        self.results = []

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def is_connected(self):
        return self.connected

    def schedule(self, func, *args, **kwargs):
        from tasq.remote.client import TasqFuture

        fut = TasqFuture()
        fut.set_result(JobResult("test", 0, True))
        if not self.is_connected():
            self._pending_jobs.append(func)
        self.results.append(True)
        return fut

    def schedule_blocking(self, func, *args, **kwargs):
        from tasq.remote.client import TasqFuture

        fut = TasqFuture()
        time.sleep(1)
        fut.set_result(JobResult("test", 0, True))
        self.results.append(True)
        return fut

    def pending_jobs(self):
        return self._pending_jobs


class TestTasqQueue(unittest.TestCase):
    def test_queue_init(self):
        tq = TasqQueue(FakeBackend())
        self.assertTrue(tq.is_connected())
        self.assertEqual(len(tq), 0)
        self.assertEqual(tq.pending_jobs(), [])

    def test_queue_disconnect(self):
        tq = TasqQueue(FakeBackend())
        self.assertTrue(tq.is_connected())
        self.assertEqual(len(tq), 0)
        self.assertEqual(tq.pending_jobs(), [])
        tq.disconnect()
        self.assertFalse(tq.is_connected())

    def test_queue_put(self):
        tq = TasqQueue(FakeBackend())
        fut = tq.put(lambda x: x + 1, 10)
        self.assertEqual(len(tq.pending_jobs()), 0)
        self.assertTrue(fut.unwrap())

    def test_queue_put_blocking(self):
        tq = TasqQueue(FakeBackend())
        t1 = time.time()
        res = tq.put_blocking(lambda x: x + 1, 10)
        t2 = time.time()
        self.assertEqual(tq.pending_jobs(), [])
        self.assertEqual(len(tq.results()), 1)
        self.assertTrue(res.unwrap())
        self.assertAlmostEqual(t2 - t1, 1, delta=0.1)


class TestMultiTasqQueue(unittest.TestCase):
    def setUp(self):
        self.backends = [FakeBackend(), FakeBackend(), FakeBackend()]
        self.router_class = RoundRobinRouter

    def test_multiqueue_init(self):
        tq = TasqMultiQueue(
            self.backends,
            lambda: actor_pool(
                num_workers=len(self.backends),
                actor_class=ClientWorker,
                router_class=self.router_class,
                clients=self.backends,
            ),
        )
        self.assertFalse(tq.is_connected())
        self.assertEqual(len(tq), 0)
        self.assertEqual(tq.pending_jobs(), [])

    def test_multiqueue_disconnect(self):
        tq = TasqMultiQueue(
            self.backends,
            lambda: actor_pool(
                num_workers=len(self.backends),
                actor_class=ClientWorker,
                router_class=self.router_class,
                clients=self.backends,
            ),
        )
        self.assertFalse(tq.is_connected())
        self.assertEqual(len(tq), 0)
        self.assertEqual(tq.pending_jobs(), [])
        tq.disconnect()
        self.assertFalse(tq.is_connected())

    def test_multiqueue_put(self):
        tq = TasqMultiQueue(
            self.backends,
            lambda: actor_pool(
                num_workers=len(self.backends),
                actor_class=ClientWorker,
                router_class=self.router_class,
                clients=self.backends,
            ),
        )
        fut = tq.put(lambda x: x + 1, 10)
        self.assertEqual(len(tq.pending_jobs()), 0)
        self.assertTrue(fut.unwrap())

    def test_multiqueue_put_blocking(self):
        tq = TasqMultiQueue(
            self.backends,
            lambda: actor_pool(
                num_workers=len(self.backends),
                actor_class=ClientWorker,
                router_class=self.router_class,
                clients=self.backends,
            ),
        )
        tq.connect()
        t1 = time.time()
        res = tq.put_blocking(lambda x: x + 1, 10)
        t2 = time.time()
        self.assertEqual(tq.pending_jobs(), [])
        self.assertEqual(len(tq.results()), 3)
        self.assertTrue(res)
        self.assertAlmostEqual(t2 - t1, .1, delta=0.1)
