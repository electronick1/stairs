import inspect

from stepist.flow.utils import validate_handler_data

from stairs.core import app_components


class Consumer(app_components.AppConsumer):
    """
    In most cases Consumer shouldn't be a worker, because it's wrapped by
    PipelineComponent -> PipelineOutput. Execution controlled inside pipeline by
    user.
    """

    def __init__(self, app, handler, as_worker=False):
        self.handler = handler
        self.as_worker = as_worker
        self.app = app

        self.step = self.app\
            .project\
            .stepist_app\
            .step(None, as_worker=as_worker)(self)

        app_components.AppConsumer.__init__(self, self.app)

    def __call__(self, **data):
        data = validate_handler_data(self.handler, data)
        output_result = self.handler(**data)

        if not self.as_worker:
            return output_result

    def __name__(self):
        module_name = inspect.getmodule(self.handler).__name__
        return "%s:%s" % (self.handler.__name__, module_name)

    def step_key(self):
        return self.__name__()

    def key(self):
        return self.name()

    def name(self):
        return self.handler.__name__

    def get_stepist_step(self):
        return self.step


