import importlib

from stairs.core.utils import AttrDict
from stairs.core.session import step_session

MODULES_FOR_IMPORT = [
    'app_config',
    'pipelines',
    'producers',
    'consumers',
]


class AppBaseComponent(object):
    """
    Base app component - class for specifying basic components methods.

    Each app component, should have stepist initialization, and object ref to
    stepist.step
    """
    app = None
    component_type = None

    def get_stepist_step(self):
        raise NotImplementedError()

    def key(self):
        raise NotImplementedError()


class AppComponents:
    """
    Aggregator of all app components.

    Also playing a role of components auto initialization (by importing modules
    inside the app)
    """

    class Components(AttrDict):

        def add_component(self, component):
            self[component.key()] = component

    def __init__(self):
        self.producers = self.Components()
        self.workers = self.Components()
        self.consumers = self.Components()
        self.flows = self.Components()
        self.steps = self.Components()

    def try_import(self, base_path):
        for module in MODULES_FOR_IMPORT:
            try:
                importlib.import_module("%s.%s" % (base_path, module))
            except ImportError as e:
                print(e)


class AppWorker(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.workers.add_component(self)


class AppProducer(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.producers.add_component(self)


class AppStep(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.steps.add_component(self)
        step_session.set_last_registered_step(self)


class AppOutput(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.consumers.add_component(self)


class AppFlow(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.flows.add_component(self)
