# -*- coding: utf-8 -*-

"""
tasq.settings.py
~~~~~~~~~~~~~~~~
"""

import os
from . import Configuration


class RemoteConfig(Configuration):

    defaults = {'debug': True}


def get_config(path=os.getenv('TASQ_REMOTE_CONF', '~/.tasq/configuration.json')):
    rc = RemoteConfig(path)
    return rc
