from stairs.core.producer.adapter import iter_adapter, iter_worker_adapter
from stairs.core.producer import Producer
from stairs.core.app.components import AppWorker
from stairs.core.output.output_model import Output
from stairs.core.output.standalone import StandAloneConsumer
from stairs.core.worker.worker import Worker


class ComponentsMixin:
    components = None

    def producer(self, *app_input):
        def _handler_wrap(handler):
            if isinstance(app_input, AppWorker):
                app_inputs = [app_input]
            else:
                app_inputs = app_input

            adapter = iter_adapter.IterAdapter(self, handler, app_inputs)
            return Producer(self, adapter=adapter)

        return _handler_wrap

    def worker_producer(self, app_input, jobs_manager=None):
        def _handler_wrap(handler):
            adapter = iter_worker_adapter.IterWorkerAdapter(app=self,
                                                            handler=handler,
                                                            app_input=app_input,
                                                            jobs_manager=jobs_manager)
            return Producer(self, adapter=adapter)

        return _handler_wrap

    def pipeline(self, config=None):
        def _deco_init(func):
            return Worker(self, func, config)

        return _deco_init

    def consumer(self):
        def _handler_wrap(func):
            return Output(app=self, handler=func)

        return _handler_wrap

    def standalone_consumer(self):
        def _handler_wrap(func):
            return StandAloneConsumer(app=self, handler=func)

        return _handler_wrap
