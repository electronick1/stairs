from stepist.flow.steps.step import StepData

import ujson


def get_connection(sqs_engine):
    queues = sqs_engine._queues.keys()
    return SQSConnection(sqs_engine.session,
                         queues,
                         sqs_engine.message_retention_period,
                         sqs_engine.visibility_timeout)


class SQSConnection:
    def __init__(self, sqs_session, queues_keys, message_retention_period,
                 visibility_timeout):
        self.sqs_session = sqs_session
        self.queues_keys = queues_keys

        self.message_retention_period = message_retention_period
        self.visibility_timeout = visibility_timeout

        self.connection = None
        self.channel = None
        self.queues = None

    def init_connection(self):
        self.sqs_client = self.sqs_session.client('sqs')
        self.sqs_resource = self.sqs_session.resource('sqs')

        for q in self.queues_keys:
            self.register_worker(q)

    def register_worker(self, queue_name):
        attrs = {}
        kwargs = {
            'QueueName': queue_name,
            'Attributes': attrs,
        }
        if self.message_retention_period is not None:
            attrs['MessageRetentionPeriod'] = str(self.message_retention_period)
        if self.visibility_timeout is not None:
            attrs['VisibilityTimeout'] = str(self.visibility_timeout)

        self.sqs_client.create_queue(**kwargs)

        queue = self.sqs_resource.get_queue_by_name(QueueName=queue_name)

        self.queues[queue_name] = queue

    def add_job(self, step_key: str, data):
        queue = self.queues.get(step_key, None)
        if not queue:
            raise RuntimeError("Queue %s not found" % step_key)

        step_data = StepData(data)
        step_data_str = ujson.dumps({'data': step_data.get_dict()})

        kwargs = {
            'MessageBody': step_data_str,
            'MessageAttributes': {},
            'DelaySeconds': 0
        }

        ret = queue.send_message(**kwargs)
        return ret['MessageId']

