from stairs.core.producer.adapter import iter_adapter, iter_worker_adapter
from stairs.core.producer import Producer
from stairs.core.app.components import AppPipeline
from stairs.core.output.output_model import Output
from stairs.core.output.standalone import StandAloneConsumer
from stairs.core.output.consumer_iter import ConsumerIter
from stairs.core.worker.worker import Pipeline


class ComponentsMixin:
    components = None

    def producer(self, *pipelines, **custom_pipelines):
        def _handler_wrap(handler) -> Producer:
            if isinstance(pipelines, AppPipeline):
                pipelines_list = [pipelines]
            else:
                pipelines_list = pipelines

            adapter = iter_adapter.IterAdapter(self,
                                               handler,
                                               pipelines_list,
                                               custom_pipelines)
            return Producer(self, adapter=adapter)

        return _handler_wrap

    def worker_producer(self, app_input, auto_init=False, jobs_manager=None):
        def _handler_wrap(handler):
            adapter = iter_worker_adapter.IterWorkerAdapter(app=self,
                                                            handler=handler,
                                                            app_input=app_input,
                                                            auto_init=auto_init,
                                                            jobs_manager=jobs_manager)
            return Producer(self, adapter=adapter)

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
