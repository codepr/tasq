# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

import json
import inspect


class Singleton(type):

    """Singleton class, just subclass this to obtain a singleton instance of an object"""

    def __init__(cls, *args, **kwargs):
        cls._instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
            return cls._instance
        return cls._instance


class SingletonArgs(type):

    """ Singleton that keep single instance for single set of arguments."""

    _instances = {}
    _init = {}

    def __init__(cls, name, bases, dct):
        cls._init[cls] = dct.get('__init__', None)

    def __call__(cls, *args, **kwargs):
        init = cls._init[cls]
        if init is not None:
            key = (cls, frozenset(inspect.getcallargs(init, None, *args, **kwargs).items()))
        else:
            key = cls

        if key not in cls._instances:
            cls._instances[key] = super(SingletonArgs, cls).__call__(*args, **kwargs)
        return cls._instances[key]


class Configuration(dict, metaclass=Singleton):

    """Configuration singleton base class. Should be subclassed to implement different
    configurations by giving a filepath to read from.

    """

    __initialized__ = False
    __conf__ = {}

    def __new__(cls, *args, **kwargs):
        if not cls.__initialized__:
            cls.__initialized__ = True
            if hasattr(cls, 'defaults'):
                setattr(cls, '_defaults', getattr(cls, 'defaults'))
            else:
                setattr(cls, '_defaults', {})
        return super().__new__(cls)

    def __init__(self, filename=None):
        if filename is not None:
            try:
                with open(filename, 'r') as conf:
                    try:
                        config_dic = json.load(conf)
                    except IOError:
                        pass
                    else:
                        self._defaults.update(config_dic)
            except FileNotFoundError:
                pass
        super().__init__(**self._defaults)

    def __repr__(self):
        return "\n".join(("{}: {}".format(k, v) for k, v in self._defaults.items()))
