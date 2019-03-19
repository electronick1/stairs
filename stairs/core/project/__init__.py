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

    def __init__(self,  stepist_app=None, config_file=None, verbose=None,
                 **config):
        project_session.set_project(self)

        self.apps = []
        self.verbose = verbose

        if config_file:
            config_file = stairs_config.ProjectConfig.load_from_file(config_file)
            config.update(config_file)

        self.config = stairs_config.ProjectConfig.init_default()
        self.config.update(config)

        self.dbs = dbs.DBs(self.config)

        if stepist_app is None:
            self.stepist_app = StepistApp(**self.config)
        else:
            self.stepist_app = stepist_app

        self.init_apps()

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

    def run_pipelines(self, custom_pipelines_to_run=None, die_when_empty=False,
                      die_on_error=True) -> None:
        """
        Iterating over apps and running (listen for jobs) pipelines
        """
        steps_to_run = []
        for p in custom_pipelines_to_run or []:
            steps_to_run.extend(p.get_workers_steps())

        steps_to_run = steps_to_run or self.steps_to_run()

        self.stepist_app.run(steps_to_run,
                             die_on_error=die_on_error,
                             die_when_empty=die_when_empty)

    def add_app(self, app: StairsApp) -> None:
        """
        Register new app in the project
        """
        self.apps.append(app)

        for app in self.apps:
            app.compile_components()

    def init_apps(self) -> None:
        """
        Iterates over all apps in config and try to import them with all
        components inside
        """
        self.apps = []
        if self.config.get('apps', None):
            for app in self.config.apps:
                stairs_app.try_to_import(app)

        for app in self.apps:
            app.compile_components()

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

    def steps_to_run(self) -> List[StepistStep]:
        components_to_run = []

        for step in self.stepist_app.get_workers_steps():
            related_to_pipeline = False

            for pipeline_step in utils.PIPELINES_STEPS_TO_RUN:
                if isinstance(step.handler, pipeline_step):
                    related_to_pipeline = True

            if related_to_pipeline:
                components_to_run.append(step)

        return components_to_run

    def set_verbose(self, verbose):
        self.verbose = verbose
        self.stepist_app.set_verbose(verbose)

    def add_job(self, pipeline_name, **data):
        pipeline = self.get_pipeline_by_name(pipeline_name)
        if pipeline is None:
            raise RuntimeError("Pipeline not found")

        pipeline.add_job(data)

    def get_producer_by_name(self, name):
        if '.' in name:
            app_name, producer_name = name.split('.')
            user_app = self.get_app_by_name(app_name)
            return user_app.components.producers[producer_name]
        else:
            producer_component = None
            for app in self.apps:
                if name in app.components.producers:
                    if producer_component is not None:
                        raise RuntimeError(
                            "There is more then one `%s` producer found, "
                            "please specified app name: app.producer_name"
                            % name
                        )
                    else:
                        # Keep producer component, as we need to check
                        # all producers for duplication
                        producer_component = app.components.producers[name]

            return producer_component

    def get_pipeline_by_name(self, name):
        if '.' in name:
            app_name, pipeline_name = name.split('.')
            user_app = self.get_app_by_name(app_name)
            return user_app.components.pipelines[pipeline_name]
        else:
            pipeline_component = None
            for app in self.apps:
                if name in app.components.pipelines:
                    if pipeline_component is not None:
                        raise RuntimeError(
                            "There is more then one `%s` pipeline found, "
                            "please specified app name: app.pipeline_name"
                            % name
                        )
                    else:
                        # Keep pipeline component, as we need to check
                        # all pipelines for duplication
                        pipeline_component = app.components.pipelines[name]

            return pipeline_component

    def get_consumer_by_name(self, name):
        if '.' in name:
            app_name, consumer_name = name.split('.')
            user_app = self.get_app_by_name(app_name)
            return user_app.components.consumers[consumer_name]
        else:
            consumer_component = None
            for app in self.apps:
                if name in app.components.consumers:
                    if consumer_component is not None:
                        raise RuntimeError(
                            "There is more then one `%s` consumer found, "
                            "please specified app name: app.consumer_name"
                            % name
                        )
                    else:
                        # Keep consumer component, as we need to check
                        # all consumers for duplication
                        consumer_component = app.components.consumers[name]

            return consumer_component
