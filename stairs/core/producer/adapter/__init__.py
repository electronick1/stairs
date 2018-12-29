

class BaseProducerAdapter(object):

    app_input = None
    app_output = None

    app = None
    handler = None

    def init_session(self):
        raise NotImplemented()

    def process(self):
        raise NotImplemented()

    def continue_process(self):
        raise NotImplemented()

    def get_redis_key(self):
        return "producer:%s:%s" % (self.app.app_name,
                                   self.handler.__name__)

