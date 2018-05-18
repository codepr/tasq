# -*- coding: utf-8 -*-

"""
tasq.settings.py
~~~~~~~~~~~~~~~~
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
from .util import Configuration


class TasqConfig(Configuration):

    defaults = {
        'host': '127.0.0.1',
        'port': 9000,
        'sign_data': False,
        'sharedkey': 'put-here-a-shared-key',
        'unix_socket': False,
        'debug': True
    }


def get_config(path=os.getenv('TASQ_CONF', '~/.tasq/configuration.json')):
    rc = TasqConfig(path)
    return rc
