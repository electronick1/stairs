from stepist import Step

from stairs.core.utils import AttrDict
from stairs.core.session import step_session


class AppBaseComponent(object):
    """
    Base class for all app components. It has specification of all default
    methods which surely used in app components.
    """

    # Stairs app
    app = None

    def get_stepist_step(self) -> Step:
        """
        Each app component, should have stepist initialization, and object ref to
        stepist.step
        """
        raise NotImplementedError()

    def key(self) -> str:
        """
        Unique id for stairs component.
        """
        raise NotImplementedError()


class AppComponents:
    """
    Aggregator of all app components.
    """

    class Components(AttrDict):

        def add_component(self, component):
            self[component.key()] = component

    def __init__(self):
        self.producers = self.Components()
        self.pipelines = self.Components()
        self.consumers = self.Components()
        self.flows = self.Components()
        self.steps = self.Components()


class AppPipeline(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.pipelines.add_component(self)


class AppProducer(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.producers.add_component(self)


class AppStep(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.steps.add_component(self)
        step_session.set_last_registered_step(self)


class AppConsumer(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.consumers.add_component(self)


class AppFlow(AppBaseComponent):
    def __init__(self, app):
        self.app = app
        app.components.flows.add_component(self)
