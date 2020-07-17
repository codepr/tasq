"""
tasq.remote.sockets.py
~~~~~~~~~~~~~~~~~~~~~~
Here are defined some wrapper for ZMQ sockets which can handle serialization
with cloudpickle
"""

import struct
import zmq
from zmq.asyncio import Socket, Context

import tasq.remote.serializer as serde
from ..settings import get_config


# Get the configuration singleton
conf = get_config()


class CloudPickleSocket(zmq.Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and
    compress data
    """

    def send_data(self, data, flags=0, signkey=None):
        """Serialize `data` with cloudpickle and compress it before sending
        through the socket
        """
        serialized = serde.dumps(data)
        if signkey:
            signed = serde.sign(signkey.encode(), serialized)
            return self.send_pyobj((signed, serialized), flags=flags)
        return self.send_pyobj(serialized, flags=flags)

    def recv_data(self, unpickle=True, flags=0, signkey=None):
        """Receive data from the socket, deserialize and decompress it with
        cloudpickle
        """
        if signkey:
            payload = self.recv_pyobj(flags)
            recv_digest, serialized = payload
            try:
                serde.verifyhmac(signkey.encode(), recv_digest, serialized)
            except serde.SignatureNotValidException:
                # TODO log here
                raise
        else:
            serialized = self.recv_pyobj(flags)
        if unpickle:
            return serde.loads(serialized)
        return serialized


class CloudPickleContext(zmq.Context):
    _socket_class = CloudPickleSocket


class AsyncCloudPickleSocket(Socket):

    """ZMQ socket adapted to send and receive cloudpickle serialized and
    compress data in an asynchronous way
    """

    async def send_data(self, data, flags=0, signkey=None):
        """Serialize `data` with cloudpickle and compress it before sending it
        asynchronously through the socket
        """
        serialized = serde.dumps(data)
        if signkey:
            signed = serde.sign(signkey.encode(), serialized)
            return await self.send_pyobj((signed, serialized), flags=flags)
        return await self.send_pyobj(serialized, flags=flags)

    async def recv_data(self, unpickle=True, flags=0, signkey=None):
        """Receive data from the socket asynchronously, deserialize and
        decompress it with cloudpickle
        """
        if signkey:
            payload = await self.recv_pyobj(flags)
            recv_digest, serialized = payload
            try:
                serde.verifyhmac(signkey.encode(), recv_digest, serialized)
            except serde.SignatureNotValidException:
                # TODO log here
                raise
        else:
            serialized = await self.recv_pyobj(flags)
        return serde.loads(serialized) if unpickle else serialized


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
        serialized = serde.dumps(data)
        if signkey:
            signed = serde.sign(signkey.encode(), serialized)
            frame = struct.pack(
                f"!H{len(signed)}s{len(serialized)}s",
                len(signed),
                signed,
                serialized,
            )
            return self._backend.put_job(frame)
        return self._backend.put_job(serialized)

    def send_result_data(self, result, signkey=None):
        zipped_result = serde.dumps(result)
        if signkey:
            signed = serde.sign(signkey.encode(), zipped_result)
            frame = struct.pack(
                f"!H{len(signed)}s{len(zipped_result)}s",
                len(signed),
                signed,
                zipped_result,
            )
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
            sign_len = struct.unpack("!H", payload[:2])
            recv_digest, serialized = struct.unpack(
                f"!{sign_len[0]}s{len(payload) - sign_len[0] - 2}s",
                payload[2:],
            )
            try:
                serde.verifyhmac(signkey.encode(), recv_digest, serialized)
            except serde.SignatureNotValidException:
                # TODO log here
                raise
        else:
            serialized = self._backend.get_next_job(timeout)

        if serialized and unpickle:
            return serde.loads(serialized)
        return serialized

    def recv_result_data(self, timeout=None, unpickle=True, signkey=None):
        if signkey:
            payload = self._backend.get_available_result(timeout)
            if not payload:
                return None
            sign_len = struct.unpack("!H", payload[:2])
            recv_digest, zipped_result = struct.unpack(
                f"!{sign_len[0]}s{len(payload) - sign_len[0] - 2}s",
                payload[2:],
            )
            try:
                serde.verifyhmac(signkey.encode(), recv_digest, zipped_result)
            except serde.SignatureNotValidException:
                # TODO log here
                raise
        else:
            zipped_result = self._backend.get_available_result(timeout)
        if zipped_result and unpickle:
            return serde.loads(zipped_result)
        return zipped_result

    def close(self):
        self._backend.close()
