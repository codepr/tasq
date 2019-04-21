"""
tasq.remote.backends.rabbitmq.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

import queue
import threading

try:
    import pika
except ImportError:
    print("You need to install pika to use rabbitmq backend")

from tasq.logger import get_logger


class RabbitMQBackend:

    """Simple Queue with RabbitMQ Backend"""

    def __init__(self, host, port, role, name, namespace=u'queue'):
        """The default connection parameters are: host='localhost', port=5672"""
        self._host, self._port = host, port
        assert role in {'receiver', 'sender'}, f"Unknown role {role}"
        self._role = role
        self._queue_name = f'{namespace}:{name}'
        self._result_name = f'{namespace}:{name}:result'
        # Blocking queues
        self._jobs = queue.Queue()
        self._results = queue.Queue()
        threading.Thread(target=self._start, daemon=True).start()

    def _get_channel(self):
        channel = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port)
        ).channel()
        return channel

    def _get_job(self, ch, method, _, body):
        print("Job incoming")
        self._jobs.put(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _get_res(self, ch, method, _, body):
        print("Result incoming")
        self._results.put(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _start(self):
        channel = self._get_channel()
        channel.basic_qos(prefetch_count=1)
        if self._role == 'receiver':
            channel.basic_consume(queue=self._queue_name,
                                  on_message_callback=self._get_job)
        else:
            channel.basic_consume(queue=self._result_name,
                                  on_message_callback=self._get_res)
        channel.start_consuming()

    def put_job(self, serialized_job):
        channel = self._get_channel()
        channel.basic_publish('', self._queue_name, serialized_job)

    def put_result(self, result):
        channel = self._get_channel()
        channel.basic_publish('', self._result_name, result)

    def get_next_job(self):
        return self._jobs.get()
        # _, _, result = next(self._consumer)
        # return result

    def get_available_result(self):
        return self._results.get()
        # _, _, result = next(self._res_consumer)
        # return result

    def close(self):
        pass
