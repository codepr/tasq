# -*- coding: utf-8 -*-

"""
tasq.remote.sockets.py
~~~~~~~~~~~~~~~~~~~~~~
Here are defined some wrapper for ZMQ sockets which can handle serialization with cloudpickle
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import zlib
import zmq
from zmq.asyncio import Socket, Context
import cloudpickle


class CloudPickleSocket(zmq.Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and compress data"""

    def send_data(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it before sending through the socket"""
        pickled_data = cloudpickle.dumps(data)
        zipped_data = zlib.compress(pickled_data)
        return self.send_pyobj(zipped_data, flags=flags)

    def recv_data(self, flags=0):
        """Receive data from the socket, deserialize and decompress it with cloudpickle"""
        zipped_data = self.recv_pyobj(flags)
        data = zlib.decompress(zipped_data)
        return cloudpickle.loads(data)


class CloudPickleContext(Context):
    _socket_class = CloudPickleSocket


class AsyncCloudPickleSocket(Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and compress data in an
    asynchronous way"""

    async def send_data(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it before sending it asynchronously
        through the socket"""
        pickled_data = cloudpickle.dumps(data)
        zipped_data = zlib.compress(pickled_data)
        return await self.send_pyobj(zipped_data, flags=flags)

    async def recv_data(self, flags=0):
        """Receive data from the socket asynchronously, deserialize and decompress it with
        cloudpickle"""
        zipped_data = await self.recv_pyobj(flags)
        data = zlib.decompress(zipped_data)
        return cloudpickle.loads(data)


class AsyncCloudPickleContext(Context):
    _socket_class = AsyncCloudPickleSocket
