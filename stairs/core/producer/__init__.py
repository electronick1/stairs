from stairs.core import app_components


class Producer(app_components.AppProducer):

    def __init__(self, app, adapter):
        self.adapter = adapter

        app_components.AppProducer.__init__(self, app)

    def __call__(self, *args, **kwargs):
        self.adapter.init_session(*args, **kwargs)

    def process(self, *args, **kwargs):
        self.adapter.process(*args, **kwargs)

    def flush_all(self):
        self.adapter.flush_all()

    def key(self):
        return self.adapter.handler.__name__

