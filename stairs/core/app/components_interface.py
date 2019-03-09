from stairs.core.producer.producer import Producer
from stairs.core.producer.batch_producer import BatchProducer
from stairs.core.output.output_model import Output
from stairs.core.output.standalone import StandAloneConsumer
from stairs.core.output.consumer_iter import ConsumerIter
from stairs.core.worker.worker import Pipeline


class ComponentsMixin:
    components = None

    def producer(self, *pipelines, **custom_pipelines):
        def _handler_wrap(handler) -> Producer:
            producer = Producer(app=self,
                                handler=handler,
                                default_callbacks=list(pipelines),
                                custom_callbacks=custom_pipelines)

            return producer

        return _handler_wrap

    def batch_producer(self, producer):
        def _handler_wrap(handler):
            batch_producer = BatchProducer(app=self,
                                           handler=handler,
                                           simple_producer=producer)
            return batch_producer
        return _handler_wrap

    def pipeline(self, config=None, queue_name=None):
        # TODO: add custom queue name for pipelines.
        def _deco_init(func):
            return Pipeline(self, func, config)

        return _deco_init

    def consumer(self):
        def _handler_wrap(func):
            return Output(app=self, handler=func)

        return _handler_wrap

    def standalone_consumer(self):
        def _handler_wrap(func):
            return StandAloneConsumer(app=self, handler=func)

        return _handler_wrap

    def consumer_iter(self):
        def _handler_wrap(func):
            return ConsumerIter(app=self, handler=func)

        return _handler_wrap

    def get_pipeline(self, name):
        return self.components.pipelines.get(name)

    def get_producer(self, name):
        return self.components.producers.get(name)

    def get_consumer(self, name):
        return self.components.consumers.get(name)
