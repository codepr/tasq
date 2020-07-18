"""
tasq.exception.py
~~~~~~~~~~~~~~~~~

All errors and exceptions are defined in this module
"""


class UnknownRunnerException(Exception):
    pass


class SignatureNotValidException(Exception):
    """Ad-hoc exception for invalid digest signature which doesn't pass the
    verification
    """

    pass


class BackendCommunicationErrorException(Exception):
    pass


class ClientNotConnectedException(Exception):
    pass
