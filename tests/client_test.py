import time
import unittest
import threading
from tasq.remote.client import Client, TasqFuture


class FakeConnection:
    def __init__(self, sleep=0):
        self.event = threading.Event()
        self.sleep = sleep
        self.result = None

    def connect(self):
        pass

    def disconnect(self):
        self.event.set()
        pass

    def recv_result(self):
        self.event.wait()
        res = self.result
        self.result = None
        return res

    def send(self, job):
        if self.sleep:
            job.add_delay(self.sleep)
        self.result = job.execute()
        self.event.set()


class TestClient(unittest.TestCase):
    def test_client_connect(self):
        client = Client(FakeConnection())
        client.connect()
        self.assertTrue(client.is_connected())
        self.assertTrue(client._gatherer is not None)
        self.assertTrue(not client._gather_loop.is_set())
        client.disconnect()

    def test_client_disconnect(self):
        client = Client(FakeConnection())
        client.connect()
        self.assertTrue(client.is_connected())
        self.assertTrue(client._gatherer is not None)
        self.assertTrue(not client._gather_loop.is_set())
        client.disconnect()
        self.assertFalse(client.is_connected())
        self.assertTrue(client._gatherer is not None)
        self.assertTrue(client._gather_loop.is_set())

    def test_client_schedule(self):
        client = Client(FakeConnection())
        client.connect()
        self.assertTrue(client.is_connected())
        self.assertTrue(client._gatherer is not None)
        self.assertTrue(not client._gather_loop.is_set())
        r = client.schedule(lambda x: x + 1, 10)
        self.assertTrue(isinstance(r, TasqFuture))
        self.assertEqual(r.unwrap(), 11)
        client.disconnect()

    def test_client_schedule_blocking(self):
        client = Client(FakeConnection(1))
        client.connect()
        self.assertTrue(client.is_connected())
        self.assertTrue(client._gatherer is not None)
        self.assertTrue(not client._gather_loop.is_set())
        t1 = time.time()
        r = client.schedule(lambda x: x + 1, 10)
        t2 = time.time()
        self.assertAlmostEqual(t2 - t1, 1, delta=0.1)
        self.assertTrue(isinstance(r, TasqFuture))
        self.assertEqual(r.unwrap(), 11)
        client.disconnect()

    def test_client_schedule_pending(self):
        client = Client(FakeConnection())
        self.assertFalse(client.is_connected())
        self.assertTrue(client._gatherer is None)
        self.assertTrue(not client._gather_loop.is_set())
        r1 = client.schedule(lambda x: x + 1, 10)
        r2 = client.schedule(lambda x: x + 2, 10)
        self.assertTrue(r1 is None)
        self.assertTrue(r2 is None)
        self.assertTrue(len(client.pending_jobs()), 2)
        client.connect()
        self.assertTrue(all(client.pending_results()))
        client.disconnect()
