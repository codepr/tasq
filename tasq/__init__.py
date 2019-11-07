from tasq.queue import TasqQueue
from tasq.jobqueue import JobQueue
from tasq.worker import ProcessQueueWorker
from tasq.remote.client import ZMQTasqClient, RedisTasqClient
from tasq.remote.supervisor import (ZMQActorSupervisor,
                                    ZMQQueueSupervisor,
                                    RedisActorSupervisor,
                                    Supervisors)

__version__ = '1.1.8'
