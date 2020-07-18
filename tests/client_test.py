import unittest
from tasq.remote.client import Client


class FakeConnection:
    def connect(self):
        pass

    def disconnect(self):
        pass

    def recv_result(self):
        pass

    def send(self, data):
        pass


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
