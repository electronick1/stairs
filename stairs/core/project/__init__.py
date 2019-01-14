import inspect

from stepist.app import App as StepistApp

from stairs.core import app as stairs_app
from stairs.core.app.components import AppBaseComponent

from stairs.core.session import project_session

from stairs.core.project import dbs, config as stairs_config
from stairs.core.project import utils


class StairsProject:

    def __init__(self,  stepist_app=None, **config):
        self.apps = []

        self.config = stairs_config.ProjectConfig.init_default()
        self.load_config(**config)

        if stepist_app is None:
            self.stepist_app = StepistApp(**self.config)
        else:
            self.stepist_app = stepist_app

        self.dbs = dbs.DBs(self.config)
        project_session.set_project(self)

    def run_pipelines(self):
        self.stepist_app.run(self.pipelines_to_run())

    def pipelines_to_run(self):
        components_to_run = []

        for step in self.stepist_app.get_workers_steps():
            related_to_pipeline = False

            for pipeline_step in utils.PIPELINES_STEPS_TO_RUN:
                if isinstance(step.handler, pipeline_step):
                    related_to_pipeline = True

            if related_to_pipeline:
                components_to_run.append(step)

        return components_to_run

    def add_app(self, app):
        self.apps.append(app)

    def load_config_from_file(self, config_path):
        self.config = stairs_config.ProjectConfig.load_from_file(config_path)
        self.load_config(**self.config)

    def load_config(self, **config):
        # update with custom config
        self.config = stairs_config.ProjectConfig(**{**self.config, **config})
        self.init_apps()

    def init_apps(self):
        self.apps = []
        if self.config.get('apps', None):
            for app in self.config.apps:
                stairs_app.try_to_import(app)

    def get_app_by_name(self, name):
        for app in self.apps:
            if app.app_name == name:
                return app

        raise RuntimeError("App '%s' not found" % name)

    def get_app_by_obj(self, obj):
        if isinstance(obj, AppBaseComponent):
            return obj.app

        if 'app' in obj.__dict__ and isinstance(obj.app, StepistApp):
            return obj.app

        module_path = inspect.getmodule(obj)
        for app in self.apps:
            if app.app_name in module_path.__name__:
                return app

        raise RuntimeError("App not found for function '%s'" % obj.__name__)

