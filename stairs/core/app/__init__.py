from importlib import util as importlib_util
import importlib

from stairs.core.utils import AttrDict

from stairs.core.session import project_session
from stairs.core.utils import signals

from stairs.core import app_components
from stairs.core.app.components_interface import ComponentsMixin


# Default app modules, which are automatically import when
# Stairs project initialized
MODULES_FOR_IMPORT = [
    'app_config',
    'pipelines',
    'producers',
    'consumers',
]


class App(ComponentsMixin):
    """
    Stairs app object collects components (e.g. pipelines, producers,
    consumers) that are similar in purpose - into one environment.

    One of the reason why Apps have a place in Stairs - it's a flexible
    customization and configuration. Stairs apps configuration allows you to
    integrate and reuse external apps in different projects.

    Each app recognized by name. This name is used to identify jobs in
    streaming service, can be used by extensions to improve debugging
    information and a lot more.

    You can add new components to your app simply wrapping your function by
    app.component decorator, for example:

        @app.pipeline()
        def my_pipeline(p): pass

    As soon as app component initialized with a function or method, it will be
    registered inside App and store at App.components field.

    You can access it like this:

        app = App()
        app_producers = app.components.producers
        app_pipelines = app.components.pipelines
        my_pipeline = app.components.pipelines.get("my_pipeline")
        my_pipeline = app.get_pipeline("my_pipeline") # shortcuts


    You can also manually add new components to the app:

        app.components.producers.add_component(some_custom_producer)


    App configuration is a simple dict like object. You can safely redefine or
    update it in any places.

        app = App()
        app.config = dict(path='/')
        app = App()
        app.config.update(path='/')
        print(app.config.path)


    Stairs App could send a signals when it's ready or initialized. Example:

        from stairs.signals import on_app_ready

        @on_app_ready("app_name")
        def init_smth(app):
            pass

    Signals:
        - on_app_created - when app instances created
        - on_app_ready - when all app compoenents compiled and ready to use

    """

    # Aggregator class with all app components
    components_cls = app_components.AppComponents

    def __init__(self,
                 app_name: str,
                 project=None,
                 config=None):
        """
        :param app_name: App name which identify your app inside stairs project
        :param project: Custom Stairs project instance
        :param config: Default app config
        """

        self.project = project
        if self.project is None:
            self.project = project_session.get_project()

        self.config = config or AttrDict()

        self.dbs = self.project.dbs
        self.components = self.components_cls()

        self.app_name = app_name

        self.project.add_app(self)

        signals.on_app_created(self.app_name).send_signal(self)

    def compile_components(self) -> None:
        """
        Init app components such as pipelines. 

        Unfortunately it's not possible to build pipelines "on fly" because
        some functions inside will not detected properly by interpreter.
        """
        for pipeline in self.components.pipelines.values():
            if pipeline.pipeline is None:
                pipeline.compile()

        signals.on_app_ready(self.app_name).send_signal(self)


def try_to_import(app_path: str) -> None:
    """
    Trying to import app by module path, it will also try to import all default
    modules.

    :param app_path: module path to stairs app
    """
    importlib.import_module(app_path)

    for module in MODULES_FOR_IMPORT:
        module_path = "%s.%s" % (app_path, module)

        spam_spec = importlib.util.find_spec(module_path)
        found = spam_spec is not None

        if not found:
            continue

        importlib.import_module(module_path)
