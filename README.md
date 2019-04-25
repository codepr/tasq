Tasq
====

Very simple distributed Task queue that allow the scheduling of job functions to be
executed on local or remote workers. Can be seen as a Proof of Concept leveraging ZMQ sockets and
cloudpickle serialization capabilities as well as a very basic actor system to handle different
loads of work from connecting clients.

Currently Tasq supports a brokerless approach through ZMQ sockets or Redis/RabbitMQ as backends.

The main advantage of using a brokerless task queue, beside latencies is the possibility of launch
and forget some workers on a network and schedule jobs to them without having them to know nothing
about the code that they will run, allowing to define tasks dinamically, without stopping the
workers. Obviously this approach open up more risks of malicious code to be injected to the workers,
currently the only security measure is to sign serialized data passed to workers, but the entire
system is meant to be used in a safe environment.

**NOTE:** The project is still in development stage and it's not advisable to try it in
production enviroments.



## Quickstart

Starting a worker on a node, with debug flag set to true on configuration file

```
$ tq redis
Listening for jobs on 127.0.0.1:9000
Response actor started
```

In a python shell

**Using a queue object**

```
Python 3.7.3 (default, Mar 26 2019, 21:43:19)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.4.0 -- An enhanced Interactive Python. Type '?' for help.
Warning: disable autoreload in ipython_config.py to improve performance.

In [1]: from tasq.queue import TasqQueue

In [2]: tq = TasqQueue(backend='redis://localhost:6379/0?name=test')

In [3]: def dup(n):
   ...:     return n * 2
   ...:

In [4]: fut = tq.put(dup, 5, name='task-01')

In [5]: fut
Out[5]: <TasqFuture at 0x7f2851826518 state=finished returned JobResult>

In [6]: fut.unwrap()
Out[6]: 10
```

**Lower-level TasqClient**

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
>>> fut = tc.schedule(foo, 5, name='Task-1')
>>> fut
>>> <Future at 0x7f7d6e048160 state=pending>
>>> fut.result
>>>
>>> # After some time, to let worker complete the job
>>> fut.result
>>> 'Foo - 2'
>>> tc.results
>>> {'Task-1': <Future at 0x7f7d6e048160 state=finished returned str>}
>>>
>>> tc.schedule_blocking(foo, 5, name='Task-2')
>>> 'Foo - 4'
>>>
>>> tc.results
>>> {'Task-1': <Future at 0x7f7d6e048160 state=finished returned str>,
>>>  'Task-2': <Future at 0x7f7d6e047268 state=finished returned str>}
```

Scheduling a job after a delay

```
>>> fut = tc.schedule(foo, 5, name='Delayed-Task', delay=5)
>>> tc.results
>>> {'Delayed-Task': <Future at 0x7f7d6e044208 state=pending>}
>>> # Wait 5 seconds
>>> tc.results
>>> {'Delayed-Task': <Future at 0x7f7d6e044208 state=finished returned str>}
>>> fut.result()
>>> 'Foo - 2'
```

Scheduling a task to be executed continously in a defined interval

```
>>> tc.schedule(foo, 5, name='8_seconds_interval_task', eta='8s')
>>> tc.schedule(foo, 5, name='2_hours_interval_task', eta='2h')
```

Delayed and interval tasks are supported even in blocking scheduling manner.

Tasq also supports an optional static configuration file, in the `tasq.settings.py` module is
defined a configuration class with some default fields. By setting the environment variable
`TASQ_CONF` it is possible to configure the location of the json configuration file on the
filesystem.

By setting the `-f` flag it is possible to also set a location of a configuration to follow on the
filesystem

```
$ tq --worker -f path/to/conf/conf.json
```

A worker can be started by specifying the type of sub worker we want:

```
$ tq --worker --worker-type process
```
Using `process` type subworker it is possible to use a distributed queue for parallel execution,
usefull when the majority of the jobs are CPU bound instead of I/O bound (actors are preferable in
that case).

Multiple workers can be started in the same node, this will start two worker process ready to
receive jobs.

```
$ tq --workers 127.0.0.1:9000:9001, 127.0.0.1:9090:9091
Listening for jobs on 127.0.0.1:9000
Listening for jobs on 127.0.0.1:9090
```

If jobs are scheduled for execution on a disconnected client, or remote workers are not up at the
time of the scheduling, all jobs will be enqeued for later execution. This means that there's no
need to actually start workers before job scheduling, at the first worker up all jobs will be sent
and executed.

### Security

Currently tasq gives the option to send pickled functions using digital sign in order to prevent
manipulations of the sent payloads, being dependency-free it uses `hmac` and `hashlib` to generate
digests and to verify integrity of payloads, planning to move to a better implementation probably
using `pynacl` or something similar.

## Behind the scenes

Essentially it is possible to start workers across the nodes of a network without forming a cluster
and every single node can host multiple workers by setting differents ports for the communication.
Each worker, once started, support multiple connections from clients and is ready to accept tasks.

Once a worker receive a job from a client, it demand its execution to dedicated actor or process,
usually selected from a pool according to a defined routing strategy in the case of actor (e.g.
Round robin, Random routing or Smallest mailbox which should give a trivial indication of the
workload of each actor and select the one with minimum pending tasks to execute) or using a simple
distributed queue across a pool of process in producer-consumer way.

![Tasq master-workers arch](static/worker_model_2.png)

Another (pool of) actor(s) is dedicated to answering the clients with the result once it is ready,
this way it is possible to make the worker listening part unblocking and as fast as possible.

The reception of jobs from clients is handled by `ZMQ.PULL` socket while the response transmission
handled by `ResponseActor` is served by `ZMQ.PUSH` socket, effectively forming a dual channel of
communication, separating ingoing from outgoing traffic.

## Installation

Being a didactical project it is not released on Pypi yet, just clone the repository and install it
locally or play with it using `python -i` or `ipython`.

```
$ git clone https://github.com/codepr/tasq.git
$ cd tasq
$ pip install .
```

or, to skip cloning part

```
$ pip install git+https://github.com/codepr/tasq.git@master#egg=tasq
```

## Changelog

See the [CHANGES](CHANGES.md) file.

## TODO:

- [ ] Possibility of a broker to persist jobs (classic task queue celery like)
- [ ] Check for pynacl for security on pickled data
- [ ] Tests
- [ ] A meaningful client pool
- [x] Debugging multiprocessing start for more workers on the same node
- [ ] Refactor of existing code and corner case handling (Still very basic implementation of even
      simple heuristics)
- [x] Delayed tasks and scheduled cron tasks
- [x] Configuration handling throughout the code
- [x] Better explanation of the implementation and actors defined
- [ ] Improve CLI options
- [ ] Dockerfile
