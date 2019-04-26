"""
tasq.remote.sockets.py
~~~~~~~~~~~~~~~~~~~~~~
Here are defined some wrapper for ZMQ sockets which can handle serialization
with cloudpickle
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import hmac
import struct
import hashlib
import zlib
import zmq
from zmq.asyncio import Socket, Context
import cloudpickle

from ..settings import get_config


# Get the configuration singleton
conf = get_config()


class InvalidSignature(Exception):

    """Ad-hoc exception for invalid digest signature which doesn't pass the
    verification
    """


def pickle_and_compress(data: object) -> bytes:
    """Pickle data with cloudpickle to bytes and then compress the resulting
    stream with zlib
    """
    pickled_data = cloudpickle.dumps(data)
    zipped_data = zlib.compress(pickled_data)
    return zipped_data


def decompress_and_unpickle(zipped_data: bytes) -> object:
    """Decompress pickled data and the unpickle it with cloudpickle"""
    data = zlib.decompress(zipped_data)
    return cloudpickle.loads(data)


def sign(sharedkey: str, pickled_data: bytes) -> bytes:
    """Generate a diget as an array of bytes and return it as a sign for the
    pickled data to sign
    """
    digest = hmac.new(sharedkey, pickled_data, hashlib.sha1).digest()
    return digest


def verifyhmac(sharedkey: str,
               recvd_digest: bytes,
               pickled_data: bytes) -> None:
    """Verify the signed pickled data is valid and no changing were made,
    otherwise raise and exception
    """
    new_digest = hmac.new(sharedkey, pickled_data, hashlib.sha1).digest()
    if recvd_digest != new_digest:
        raise InvalidSignature


class CloudPickleSocket(zmq.Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and
    compress data
    """

    def send_data(self, data, flags=0, signkey=None):
        """Serialize `data` with cloudpickle and compress it before sending
        through the socket
        """
        zipped_data = pickle_and_compress(data)
        if signkey:
            signed = sign(signkey.encode(), zipped_data)
            return self.send_pyobj((signed, zipped_data), flags=flags)
        return self.send_pyobj(zipped_data, flags=flags)

    def recv_data(self, unpickle=True, flags=0, signkey=None):
        """Receive data from the socket, deserialize and decompress it with
        cloudpickle
        """
        if signkey:
            payload = self.recv_pyobj(flags)
            recv_digest, zipped_data = payload
            try:
                verifyhmac(signkey.encode(), recv_digest, zipped_data)
            except InvalidSignature:
                # TODO log here
                raise
        else:
            zipped_data = self.recv_pyobj(flags)
        if unpickle:
            return decompress_and_unpickle(zipped_data)
        return zipped_data


class CloudPickleContext(Context):
    _socket_class = CloudPickleSocket


class AsyncCloudPickleSocket(Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and
    compress data in an asynchronous way
    """

    async def send_data(self, data, flags=0, signkey=None):
        """Serialize `data` with cloudpickle and compress it before sending it
        asynchronously through the socket
        """
        zipped_data = pickle_and_compress(data)
        if signkey:
            signed = sign(signkey.encode(), zipped_data)
            return await self.send_pyobj((signed, zipped_data), flags=flags)
        return await self.send_pyobj(zipped_data, flags=flags)

    async def recv_data(self, unpickle=True, flags=0, signkey=None):
        """Receive data from the socket asynchronously, deserialize and
        decompress it with cloudpickle
        """
        if signkey:
            payload = await self.recv_pyobj(flags)
            recv_digest, zipped_data = payload
            try:
                verifyhmac(signkey.encode(), recv_digest, zipped_data)
            except InvalidSignature:
                # TODO log here
                raise
        else:
            zipped_data = await self.recv_pyobj(flags)
        if unpickle:
            return decompress_and_unpickle(zipped_data)
        return zipped_data


class AsyncCloudPickleContext(Context):
    _socket_class = AsyncCloudPickleSocket


class BackendSocket:

    def __init__(self, backend):
        self._backend = backend

    def get_pending_jobs(self):
        """Return a list of pending jobs, or either a tuple with a list of
        pending jobs and a list of working pending jobs (jobs that are started
        but are still in execution phase)
        """
        return self._backend.get_pending_jobs()

    def send_data(self, data, signkey=None):
        """Serialize `data` with cloudpickle and compress it before sending
        through the socket
        """
        zipped_data = pickle_and_compress(data)
        if signkey:
            signed = sign(signkey.encode(), zipped_data)
            frame = struct.pack(f'!H{len(signed)}s{len(zipped_data)}s',
                                len(signed), signed, zipped_data)
            return self._backend.put_job(frame)
        return self._backend.put_job(zipped_data)

    def send_result_data(self, result, signkey=None):
        zipped_result = pickle_and_compress(result)
        if signkey:
            signed = sign(signkey.encode(), zipped_result)
            frame = struct.pack(f'!H{len(signed)}s{len(zipped_result)}s',
                                len(signed), signed, zipped_result)
            return self._backend.put_result(frame)
        return self._backend.put_result(zipped_result)

    def recv_data(self, timeout=None, unpickle=True, signkey=None):
        """Receive data from the socket, deserialize and decompress it with
        cloudpickle
        """
        if signkey:
            payload = self._backend.get_next_job(timeout)
            if not payload:
                return None
            sign_len = struct.unpack('!H', payload[:2])
            recv_digest, zipped_data = struct.unpack(
                f'!{sign_len[0]}s{len(payload) - sign_len[0] - 2}s',
                payload[2:]
            )
            try:
                verifyhmac(signkey.encode(), recv_digest, zipped_data)
            except InvalidSignature:
                # TODO log here
                raise
        else:
            zipped_data = self._backend.get_next_job(timeout)

        if zipped_data and unpickle:
            return decompress_and_unpickle(zipped_data)
        return zipped_data

    def recv_result_data(self, timeout=None, unpickle=True, signkey=None):
        if signkey:
            payload = self._backend.get_available_result(timeout)
            if not payload:
                return None
            sign_len = struct.unpack('!H', payload[:2])
            recv_digest, zipped_result = struct.unpack(
                f'!{sign_len[0]}s{len(payload) - sign_len[0] - 2}s',
                payload[2:]
            )
            try:
                verifyhmac(signkey.encode(), recv_digest, zipped_result)
            except InvalidSignature:
                # TODO log here
                raise
        else:
            zipped_result = self._backend.get_available_result(timeout)
        if zipped_result and unpickle:
            return decompress_and_unpickle(zipped_result)
        return zipped_result
