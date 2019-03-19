from stairs.core.producer.producer import Producer
from stairs.core.producer.batch_producer import BatchProducer
from stairs.core.output.output_model import Output
from stairs.core.output.standalone import StandAloneConsumer
from stairs.core.output.consumer_iter import ConsumerIter
from stairs.core.worker.worker import Pipeline


class ComponentsMixin:
    components = None

    def producer(self, *pipelines, custom: list=None) -> Producer:
        def _handler_wrap(handler) -> Producer:
            custom_callbacks = custom or self.components.pipelines.values()
            producer = Producer(app=self,
                                handler=handler,
                                default_callbacks=list(pipelines),
                                custom_callbacks=custom_callbacks)

            return producer

        return _handler_wrap

    def batch_producer(self, producer: Producer) -> BatchProducer:
        def _handler_wrap(handler):
            batch_producer = BatchProducer(app=self,
                                           handler=handler,
                                           simple_producer=producer)
            return batch_producer
        return _handler_wrap

    def spark_producer(self, *pipelines, custom: list=None):
        from stairs.core.producer.spark_producer import SparkProducer

        def _handler_wrap(handler) -> Producer:
            custom_callbacks = custom or self.components.pipelines.values()
            producer = SparkProducer(app=self,
                                     handler=handler,
                                     default_callbacks=list(pipelines),
                                     custom_callbacks=custom_callbacks)

            return producer

        return _handler_wrap

    def pipeline(self, config=None) -> Pipeline:
        # TODO: add custom queue name for pipelines.
        def _deco_init(func):
            return Pipeline(self, func, config)

        return _deco_init

    def consumer(self) -> Output:
        def _handler_wrap(func):
            return Output(app=self, handler=func)

        return _handler_wrap

    def standalone_consumer(self) -> StandAloneConsumer:
        def _handler_wrap(func):
            return StandAloneConsumer(app=self, handler=func)

        return _handler_wrap

    def consumer_iter(self) -> ConsumerIter:
        def _handler_wrap(func):
            return ConsumerIter(app=self, handler=func)

        return _handler_wrap

    def get_pipeline(self, name) -> Pipeline:
        return self.components.pipelines.get(name)

    def get_producer(self, name):
        return self.components.producers.get(name)

    def get_consumer(self, name):
        return self.components.consumers.get(name)
