import inspect

from typing import List

from stepist.app import App as StepistApp
from stepist.flow.steps.step import Step as StepistStep

from stairs.core import app as stairs_app
from stairs.core.app import App as StairsApp
from stairs.core.app.components import AppBaseComponent

from stairs.core.session import project_session

from stairs.core.project import dbs, config as stairs_config
from stairs.core.project import utils


class StairsProject:

    def __init__(self,  stepist_app=None, config_file=None, **config):
        self.apps = []

        if config_file:
            config_file = stairs_config.ProjectConfig.load_from_file(config_file)
            config.update(config_file)

        self.config = stairs_config.ProjectConfig.init_default()
        self._update_config(**config)

        if stepist_app is None:
            self.stepist_app = StepistApp(**self.config)
        else:
            self.stepist_app = stepist_app

        self.dbs = dbs.DBs(self.config)
        project_session.set_project(self)

    @classmethod
    def init_from_file(cls, config_path: str, **init_kwargs):
        config = stairs_config.ProjectConfig.load_from_file(config_path)
        return cls(**init_kwargs, **config)

    def _update_config(self, **config) -> None:
        """
        Updating current config with a new one.

        Should be executed before Stepist App initialized
        """
        # update with custom config
        self.config = stairs_config.ProjectConfig(**{**self.config, **config})
        self.init_apps()

    def run_pipelines(self, die_when_empty=False, die_on_error=True) -> None:
        """
        Iterating over apps and running (listen for jobs) pipelines
        """
        self.stepist_app.run(self.pipelines_to_run(),
                             die_on_error=die_on_error,
                             die_when_empty=die_when_empty)

    def add_app(self, app: StairsApp) -> None:
        """
        Register new app in the project
        """
        self.apps.append(app)

    def init_apps(self) -> None:
        """
        Iterates over all apps in config and try to import them with all
        components inside
        """
        self.apps = []
        if self.config.get('apps', None):
            for app in self.config.apps:
                stairs_app.try_to_import(app)

    def get_app(self, name) -> StairsApp:
        """
        Just shortcust for get_app_by_name
        """
        return self.get_app_by_name(name)

    def get_app_by_name(self, name: str):
        for app in self.apps:
            if app.app_name == name:
                return app

        raise RuntimeError("App '%s' not found" % name)

    def get_app_by_obj(self, obj) -> StairsApp:
        """
        Trying to guess app by some pyobject
        """
        if isinstance(obj, AppBaseComponent):
            return obj.app

        if 'app' in obj.__dict__ and isinstance(obj.app, StepistApp):
            return obj.app

        module_path = inspect.getmodule(obj)
        for app in self.apps:
            if app.app_name in module_path.__name__:
                return app

        raise RuntimeError("App not found for function '%s'" % obj.__name__)

    def pipelines_to_run(self) -> List[StepistStep]:
        components_to_run = []

        for step in self.stepist_app.get_workers_steps():
            related_to_pipeline = False

            for pipeline_step in utils.PIPELINES_STEPS_TO_RUN:
                if isinstance(step.handler, pipeline_step):
                    related_to_pipeline = True

            if related_to_pipeline:
                components_to_run.append(step)

        return components_to_run

