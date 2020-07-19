"""
tasq.remote.serializer.py
~~~~~~~~~~~~~~~~~~~~~~~~~

Marshalling functions here, including signature valdiation and checks
"""

import hmac
import zlib
import hashlib
from typing import Any
import cloudpickle
from ..exception import SignatureNotValidException


def dumps(data: Any) -> bytes:
    """Pickle data with cloudpickle to bytes and then compress the resulting
    stream with zlib
    """
    pickled_data = cloudpickle.dumps(data)
    zipped_data = zlib.compress(pickled_data)
    return zipped_data


def loads(zipped_data: bytes) -> Any:
    """Decompress pickled data and the unpickle it with cloudpickle"""
    data = zlib.decompress(zipped_data)
    return cloudpickle.loads(data)


def sign(key: bytes, pickled_data: bytes) -> bytes:
    """Generate a diget as an array of bytes and return it as a sign for the
    pickled data to sign
    """
    digest = hmac.new(key, pickled_data, hashlib.sha1).digest()
    return digest


def verifyhmac(key: bytes, recvd_digest: bytes, pickled_data: bytes) -> None:
    """Verify the signed pickled data is valid and no changing were made,
    otherwise raise and exception
    """
    new_digest = hmac.new(key, pickled_data, hashlib.sha1).digest()
    if recvd_digest != new_digest:
        raise SignatureNotValidException
