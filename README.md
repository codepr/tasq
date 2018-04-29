Tasq
====

Very simple broker-less distributed Task queue that allow the scheduling of jobs function to be
executed on local or remote workers. Proof of Concept leveraging ZMQ sockets and cloudpickle
serialization capabilities as well as a very basic actor system to handle different loads of work
from connecting clients.


## Quickstart

Starting a worker on a node, with debug flag set to true on configuration file

```sh
$ tq --worker
DEBUG - Push channel set to 127.0.0.1:9000
DEBUG - Pull channel set to 127.0.0.1:9001
DEBUG - MainThread - Response actor started
```

In a python shell

```
Python 3.6.5 (default, Apr 12 2018, 22:45:43)
[GCC 7.3.1 20180312] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from tasq import TasqClient
>>> tc = TasqClient('127.0.0.1', 9000)
>>> tc.connect()
>>>
>>> def foo(num):
>>>     import time
>>>     import random
>>>     r = random.randint(0, 2)
>>>     time.sleep(r)
>>>     return f'Foo - {random.randint(0, num)}'
>>>
>>> tc.schedule(foo, 5, name='Task-1')
>>> tc.results
>>> {'Task-1': 'Foo - 3'}
>>>
>>> tc.schedule_blocking(foo, 5, name='Task-2')
>>> ('Task-2', 'Foo - 4')
>>>
>>> tc.results
>>> {'Task-1': 'Foo - 3', 'Task-2': 'Foo - 4'}
```

## Installation

Being a didactical project it is not released on Pypi yet, just clone the repository and install it
locally or play with it using `python -i` or `ipython`.

```sh
$ git clone https://github.com/codepr/tasq.git
$ cd tasq
$ pip install .
```

or, to skip cloning part

```sh
$ pip install -e git+https://github.com/codepr/tasq.git@master
```

## Changelog

See the [CHANGES](CHANGES.md) file.

## TODO:

- An useful client pool
- Refactor of existing code and corner case handling (Still very basic implementation of even simple
  heuristics)
- Configuration handling throughout the code
- Better explanation of the implementation and actors defined
