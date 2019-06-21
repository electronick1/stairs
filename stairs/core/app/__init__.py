import importlib

from stairs.core.utils import AttrDict

from stairs.core.session import project_session
from stairs.core.project import signals

from stairs.core.app import components
from stairs.core.app.components_interface import ComponentsMixin


# Default app modules to automatically import when Stairs project initialized
MODULES_FOR_IMPORT = [
    'app_config',
    'pipelines',
    'producers',
    'consumers',
]


class App(ComponentsMixin):
    """

    """

    # app name, extracted from app_package
    app_name = None

    # Aggregator class with all app components
    components = None

    # Project, with full configuration.
    project = None

    def __init__(self,
                 app_name: str,
                 project=None):

        self.project = project
        if self.project is None:
            self.project = project_session.get_project()

        self.config = AttrDict()

        self.dbs = self.project.dbs
        self.components = components.AppComponents()

        self.app_name = app_name

        self.add_to_project()

    def __dir__(self):
        """
        Me want to make public only components which used in Queue Manager or
        in cli.

        TODO: make better implementation of public components for app object.
        """
        return list(self.components.producers.keys()) + \
               list(self.components.pipelines.keys())

    def compile_components(self):
        """
        Init app components such as pipelines. 

        Unfortunatelly it's not possible to build pipelines "on fly" because
        some functions inside will not detected properly by python.
        """
        for pipeline in self.components.pipelines.values():
            if pipeline.pipeline is None:
                pipeline.compile()

        signals.on_app_ready(self.app_name).send_signal()

    def add_to_project(self):
        self.project.add_app(self)


def try_to_import(app_path: str):
    """

    :param app_path:
    :return:
    """
    importlib.import_module(app_path)

    for module in MODULES_FOR_IMPORT:
        module_path = "%s.%s" % (app_path, module)

        spam_spec = importlib.util.find_spec(module_path)
        found = spam_spec is not None

        if not found:
            continue

        importlib.import_module(module_path)
