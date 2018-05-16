# -*- coding: utf-8 -*-

"""
tasq.remote.sockets.py
~~~~~~~~~~~~~~~~~~~~~~
Here are defined some wrapper for ZMQ sockets which can handle serialization with cloudpickle
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import hmac
import hashlib
import zlib
import zmq
from zmq.asyncio import Socket, Context
import cloudpickle

from ..settings import get_config


# Get the configuration singleton
conf = get_config()


def pickle_and_compress(data):
    """Pickle data with cloudpickle to bytes and then compress the resulting stream with zlib"""
    pickled_data = cloudpickle.dumps(data)
    zipped_data = zlib.compress(pickled_data)
    return zipped_data


def decompress_and_unpickle(zipped_data):
    """Decompress pickled data and the unpickle it with cloudpickle"""
    data = zlib.decompress(zipped_data)
    return cloudpickle.loads(data)


def sign(sharedkey, pickled_data):
    """Generate a diget as an array of bytes and return it as a sign for the pickled data to sign"""
    digest = hmac.new(sharedkey, pickled_data, hashlib.sha1).digest()
    return digest


def verifyhmac(sharedkey, recvd_digest, pickled_data):
    """Verify the signed pickled data is valid and no changing were made, otherwise raise and
    exception"""
    new_digest = hmac.new(sharedkey, pickled_data, hashlib.sha1).digest()
    if recvd_digest != new_digest:
        raise TypeError


class CloudPickleSocket(zmq.Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and compress data"""

    def send_data(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it before sending through the socket"""
        zipped_data = pickle_and_compress(data)
        return self.send_pyobj(zipped_data, flags=flags)

    def send_signed(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it, after that, sign the generated payload
        and send it through the socket"""
        zipped_data = pickle_and_compress(data)
        signed = sign(conf['sharedkey'].encode(), zipped_data)
        return self.send_pyobj((signed, zipped_data), flags=flags)

    def recv_data(self, flags=0):
        """Receive data from the socket, deserialize and decompress it with cloudpickle"""
        zipped_data = self.recv_pyobj(flags)
        return decompress_and_unpickle(zipped_data)

    def recv_signed(self, flags=0):
        """Receive data from the socket, check the digital signature in order to verify the
        integrity of data and the that the sender is allowed to talk to us, deserialize and
        decompress it with cloudpickle"""
        payload = self.recv_pyobj(flags)
        recv_digest, pickled_data = payload
        try:
            verifyhmac(conf['sharedkey'].encode(), recv_digest, pickled_data)
        except TypeError:
            raise
        else:
            return decompress_and_unpickle(pickled_data)


class CloudPickleContext(Context):
    _socket_class = CloudPickleSocket


class AsyncCloudPickleSocket(Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and compress data in an
    asynchronous way"""

    async def send_data(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it before sending it asynchronously
        through the socket"""
        zipped_data = pickle_and_compress(data)
        return await self.send_pyobj(zipped_data, flags=flags)

    async def send_signed(self, data, flags=0):
        """Serialize `data` with cloudpickle and compress it, after that, sign the generated payload
        and send it asynchronously through the socket"""
        zipped_data = pickle_and_compress(data)
        signed = sign(conf['sharedkey'].encode(), zipped_data)
        return await self.send_pyobj((signed, zipped_data), flags=flags)

    async def recv_data(self, flags=0):
        """Receive data from the socket asynchronously, deserialize and decompress it with
        cloudpickle"""
        zipped_data = await self.recv_pyobj(flags)
        return decompress_and_unpickle(zipped_data)

    async def recv_signed(self, flags=0):
        """Receive data from the socket asynchronously, check the digital signature in order to
        verify the integrity of data and the that the sender is allowed to talk to us, deserialize
        and decompress it with cloudpickle"""
        payload = await self.recv_pyobj(flags)
        recv_digest, pickled_data = payload
        try:
            verifyhmac(conf['sharedkey'].encode(), recv_digest, pickled_data)
        except TypeError:
            raise
        else:
            return decompress_and_unpickle(pickled_data)


class AsyncCloudPickleContext(Context):
    _socket_class = AsyncCloudPickleSocket
