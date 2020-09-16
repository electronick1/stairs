from stairs.core.session import signals_session


def on_project_ready():
    return Signal("on_project_created")


def on_app_ready(app_name: str):
    # Return app object inside signal handler
    return Signal("on_app_created:%s" % app_name)


def on_app_created(app_name: str):
    # Return app object inside signal handler
    return Signal("on_app_created:%s" % app_name)


def on_pipeline_ready(pipeline):
    # Return pipeline object inside signal handler
    return Signal("on_pipeline_compiled:%s" % pipeline.__name__())


def on_component_called():
    return Signal("on_component_called")


def on_component_finished():
    return Signal("on_component_finished")


class Signal:

    def __init__(self, key):
        self.key = key

    def __call__(self, handler):
        signals_session.add_signal(self.key, handler)
        return handler

    def send_signal(self, *args, **kwargs):
        for h in signals_session.get_signals(self.key):
            h(*args, **kwargs)
