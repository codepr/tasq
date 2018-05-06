### 0.6.0
(May 6, 2018)

- Refactored client code, now it uses a Future system to handle results and return a future even
  while scheduling a job in a non-blocking manner
- Improved logging
- Improved representation of a Job in string

### 0.5.0
(May 5, 2018)

- Added first implementation of delayed jobs
- Added first implementation of interval-scheduled jobs
- Added a basic ActorSystem like and context to actors
- Refactored some parts, removed Singleton and Configuration classes from __init__.py

### 0.3.0:
(May 1, 2018)

- Fixed minor bug in initialization of multiple workers on the same node
- Added support for pending tasks on the client side

### 0.2.0:
(Apr 30, 2018)

- Renamed some modules
- Added basic logging to modules
- Defined a client supporting sync and async way of scheduling jobs
- Added routing logic for worker actors
- Refactored code

### 0.1.2
(Apr 29, 2018)

- Added asynchronous way of handling communication on ZMQ sockets

### 0.1.1:
(Apr 28, 2018)

- Switch to PUSH/PULL pattern offered by ZMQ
- Subclassed ZMQ sockets in order to handle cloudpickle serialization

### 0.1.0:

(Apr 26, 2018)

- First unfinished version, WIP
