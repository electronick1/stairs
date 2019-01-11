import inspect
import importlib

from stairs.core.utils import AttrDict

from stairs.core.app import components
from stairs.core.app.components_interface import ComponentsMixin
from stairs.core.session import project_session


MODULES_FOR_IMPORT = [
    'app_config',
    'pipelines',
    'producers',
    'consumers',
]


class App(ComponentsMixin):

    """
    Used for collect all components and manage everything inside project.
    """

    # app name, extracted from app_package
    app_name = None

    # Aggregator class with all app components
    components = None

    # Project, with full configuration.
    project = None

    def __init__(self, app_name, project=None):

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
               list(self.components.workers.keys())

    def add_to_project(self):
        self.project.add_app(self)


def try_to_import(app_path):
    app_package = importlib.import_module(app_path)

    for module in MODULES_FOR_IMPORT:
        module_path = "%s.%s" % (app_path, module)

        spam_spec = importlib.util.find_spec(module_path)
        found = spam_spec is not None

        if not found:
            continue

        importlib.import_module(module_path)


