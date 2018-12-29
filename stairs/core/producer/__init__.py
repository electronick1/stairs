from stairs.core.app import components


class Producer(components.AppProducer):

    def __init__(self, app, adapter):
        self.adapter = adapter

        components.AppProducer.__init__(self, app)

    def __call__(self):
        self.adapter.init_session()

    def process(self):
        self.adapter.process()

    def key(self):
        return self.adapter.handler.__name__

