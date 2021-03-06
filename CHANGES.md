## 0.10.2 - Apr 26, 2019

- Refactored CLI commands and logging format
- Added factory methods to supervisors
- Fixed bug in rabbitmq backend module

## 0.10.1 - Apr 26, 2019

- Moved factory logic for client creation to from_url method on client module
- Added TasqFuture result from clients result, to return more structured results
  with additional informations about execution.

## 0.10.0 - Apr 22, 2019

- Added a TasqQueue class for more convenient uses
- Fixed some bugs
- Renamed `master` -> `supervisor`
- Added RabbitMQ to supported backends, still working on a common interface
- Refactored some parts on connection

## 0.9.0 - Mar 23, 2019

- Refactored log system
- Started backend broker support for job queues and persistence
- Add redis client

## 0.8.0 - Jul 15, 2018

- Added repeated jobs capabilities to process/thread queue workers too
  (Previously only Actor worker could achieve that)
- Fixed some bugs, renamed `ProcessWorker` -> `QueueWorker` and
  `ProcessMaster` -> `QueueMaster`

## 0.7.0 - Jul 14, 2018

- Added the possibility to choose the type of workers of each master process,
  can be either a pool of actors or a pool of processes, based on the nature of
  the majority of the jobs that need to be executed. A majority of I/O bound
  operations should stick to `ActorMaster` type workers, in case of CPU bound
  tasks `QueueMaster` should give better results.

## 0.6.1 - May 18, 2018

- Decoupled connection handling from `tasq.remote.master` and `tasq.remote.client`
  into a dedicated module tasq.remote.connection

## 0.6.0 - May 17, 2018

- Simple implementation of digital signed data sent through sockets, this way
  sender and receiver have a basic security layer to check for integrity and
  legitimacy of received data

## 0.5.0 - May 14, 2018

- Added a ClientPool implementation to schedule jobs to different workers by
  using routers capabilities

## 0.4.0 - May 6, 2018

- Refactored client code, now it uses a Future system to handle results and
  return a future even while scheduling a job in a non-blocking manner
- Improved logging
- Improved representation of a Job in string

## 0.3.0 - May 5, 2018

- Added first implementation of delayed jobs
- Added first implementation of interval-scheduled jobs
- Added a basic ActorSystem like and context to actors

- Refactored some parts, removed Singleton and Configuration classes from
  __init__.py

## 0.2.1 - May 1, 2018

- Fixed minor bug in initialization of multiple workers on the same node
- Added support for pending tasks on the client side

## 0.2.0 - Apr 30, 2018

- Renamed some modules
- Added basic logging to modules
- Defined a client supporting sync and async way of scheduling jobs
- Added routing logic for worker actors
- Refactored code

## 0.1.2 - Apr 29, 2018

- Added asynchronous way of handling communication on ZMQ sockets

## 0.1.1 - Apr 28, 2018

- Switch to PUSH/PULL pattern offered by ZMQ
- Subclassed ZMQ sockets in order to handle cloudpickle serialization

## 0.1.0 - Apr 26, 2018

- First unfinished version, WIP
