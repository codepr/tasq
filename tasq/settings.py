# -*- coding: utf-8 -*-

"""
tasq.settings.py
~~~~~~~~~~~~~~~~
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
from . import Configuration


class TasqConfig(Configuration):

    defaults = {
        'host': '127.0.0.1',
        'port': 9000,
        'debug': True
    }


def get_config(path=os.getenv('TASQ_CONF', '~/.tasq/configuration.json')):
    rc = TasqConfig(path)
    return rc
