import ujson
from stepist.flow.steps.step import StepData

from redis import ConnectionPool, Redis


def get_connection(redis_engine):
    pool = redis_engine.redis_connection.connection_pool

    return RedisConnection(
        connection_class=pool.connection_class,
        connection_kwargs=pool.connection_kwargs,
        max_connections=pool.max_connections,
    )


class RedisConnection:
    def __init__(self, connection_class, connection_kwargs, max_connections):
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        self.connection = None
        self.channel = None
        self.queues = None

    def init_connection(self):
        self.connection = Redis(connection_pool=ConnectionPool(
            connection_class=self.connection_class,
            max_connections=self.max_connections,
            **self.connection_kwargs
        ))

    def add_job(self, step_key: str, data):
        step_data = StepData(data)
        step_data_str = ujson.dumps({'data': step_data.get_dict()})
        self.connection.lpush(self.redis_queue_key(step_key), step_data_str)

    def redis_queue_key(self, job_key):
        return "stepist::%s" % job_key

