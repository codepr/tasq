"""
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import errno
import logging
from logging.handlers import RotatingFileHandler


DEFAULT_LOGSIZE = int(os.getenv('LOGSIZE', '5242880'))
DEFAULT_LOGPATH = os.getenv('LOGPATH', '/tmp/log/tasq')
DEFAULT_FORMAT = os.getenv('LOGFMT', '%(name)s - %(message)s')
DEFAULT_LOGLEVEL = os.getenv('LOGLVL', 'INFO')


LOGLVLMAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR
}

# Set default logging handler to avoid "No handler found" warnings.
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())


def _make_dir(path, logpath):
    """"""
    try:
        os.makedirs(os.path.join(logpath, path), exist_ok=True)
    except TypeError:
        try:
            os.makedirs(os.path.join(logpath, path))
        except OSError as ex:
            if ex.errno == errno.EEXIST and os.path.isdir(os.path.join(logpath, path)):
                pass
            else:
                raise


class MakeCappedFileHandler(RotatingFileHandler):
    """
    """
    def __init__(self, filename, max_size=DEFAULT_LOGSIZE, backup_count=2,
                 logpath=DEFAULT_LOGPATH, mode='a', encoding=None, delay=0):
        _make_dir(os.path.dirname(filename), logpath)
        RotatingFileHandler.__init__(self, filename, mode, max_size,
                                     backup_count, encoding, delay)


def get_logger(name, loglevel=DEFAULT_LOGLEVEL,
               fmt=DEFAULT_FORMAT, logpath=DEFAULT_LOGPATH):

    # create module logger
    logger = logging.getLogger(name)
    logger.setLevel(LOGLVLMAP[loglevel])

    if not logger.handlers:
        # create file handler which logs even debug messages
        fh = MakeCappedFileHandler(os.path.join(logpath, f'{name}.log'))
        fh.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(LOGLVLMAP[loglevel])

        # create formatter and add it to the handlers
        formatter = logging.Formatter(fmt)
        fh.setFormatter(logging.Formatter('%(asctime)s - ' + fmt))
        ch.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger


log = get_logger(__name__)
