from stepist.flow.steps.step import StepData

import ujson
import pika


def get_connection(rmq_engine):
    impl = rmq_engine.pika_connection._impl
    return RMQConnection(impl.__class__, impl.params)


class RMQConnection:
    def __init__(self, _impl_class, params):
        self._impl_class = _impl_class
        self.params = params

        self.connection = None
        self.channel = None
        self.queues = None

    def init_connection(self):
        self.connection = pika.BlockingConnection(_impl_class=self._impl_class,
                                                  parameters=self.params)
        self.channel = self.connection.channel()

    def register_queue(self, step_key):
        self.channel.queue_declare(queue=step_key,
                                   auto_delete=False,
                                   passive=True)

    def add_job(self, step_key: str, data):
        if step_key not in self.queues:
            self.register_queue(step_key)

        step_data = StepData(data)
        step_data_str = ujson.dumps({'data': step_data.get_dict()})

        self.channel.basic_publish(
            exchange='',
            routing_key=step_key,
            body=step_data_str
        )

