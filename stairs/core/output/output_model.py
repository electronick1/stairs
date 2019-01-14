import inspect

from stairs.core.app import components


class Output(components.AppOutput):

    def __init__(self, app, handler, as_daemon=False):
        self.handler = handler
        self.as_daemon = as_daemon
        self.app = app

        self.step = self.app\
            .project\
            .stepist_app\
            .step(None, as_worker=as_daemon)(self)

        components.AppOutput.__init__(self, self.app)

    def __call__(self, **data):
        output_result = self.handler(**data)

        if not self.as_daemon:
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


