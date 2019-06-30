import inspect
import ujson

from typing import List

from stepist.app import App as StepistApp
from stepist.flow.steps.step import Step as StepistStep

from stairs.core import app as stairs_app
from stairs.core.app import App as StairsApp
from stairs.core.app_components import AppBaseComponent, AppPipeline

from stairs.core.session import project_session

from stairs.core.project import dbs, config as stairs_config
from stairs.core.project import utils
from stairs.core.utils import signals


class StairsProject:
    """ StairsProject is the main control component for stairs. It's stores
    configs files, apps, and db connections. It allows you to run app
    components and get access to different parts of the system.

    Stairs project consist of a set of Apps. Stairs apps were invited for
    extensibility and flexibility, similar to Flask/Django philosophy.
    Each stairs app has several components which represents data pipelines and
    ETL concepts.

    To create StairsProject you can just create StairsProject instance with
    default args:

    ```
    project = StairsProject()
    ```

    The StairsProject instance (project) will be stored globally and you can
    always access it using stairs sessions:

    ```
    from stairs import get_project

    project = get_project()
    ```

    Stairs communication part is based on stepist. Stepist app allows you to
    control messaging and streaming between pipelines functions.

    To control communication part inside Stairs, just define StepistApp
    ```
    from stepist.app import App
    stepist = App()
    ```

    If you want to change streaming/queues service define custom one:
    ```
    from stepist import App as StepistApp
    from stepist import RQAdapter, SQSAdapter, RedisAdapter

    stairs_project = StairsProject(StepistApp(SQSAdapter()))
    ```

    Check more about stepist -> https://github.com/electronick1/stepist

    """

    def __init__(self,
                 stepist_app: StepistApp = None,
                 config_file: str = None,
                 verbose: bool = None,
                 data_pickeler=ujson,
                 use_booster: bool = False,
                 **config):

        """
        :param stepist_app: stepist.App instance, with configuration
        for components communication.

        :param config_file: path to config file, used for populating
        project.config

        :param verbose: True - if you need to print status and meta information

        :param data_pickeler: Class with loads/dumps methods to
        encode/decode data, used to compress/convert data during communication
        between components.

        :param use_booster: True - if you need to use Stepist booster
        communication.

        :param config: additional configs variables for Stairs project.
        """

        # Set project globally in thread-safe session.
        project_session.set_project(self)

        self.apps = []
        self.verbose = verbose
        self.data_pickler = data_pickeler

        if config_file:
            config_file = stairs_config.ProjectConfig.load_from_file(config_file)
            config.update(config_file)

        self.config = stairs_config.ProjectConfig.init_default()
        self.config.update(config)

        self.dbs = dbs.DBs(self.config)

        # Setup Stepist app and internal communication parts
        if stepist_app is None:
            self.stepist_app = StepistApp(data_pickeler=data_pickeler,
                                          use_booster=use_booster,
                                          **self.config)
        else:
            self.stepist_app = stepist_app

        # Init all apps and components defined in config object, it will try to
        # import default app modules as well.
        self.init_apps()

        signals.on_project_ready().send_signal()

    def _update_config(self, **config) -> None:
        """
        Updating current config object with a new arguments.

        (!) Should be executed before Stepist App initialized.

        :param config: dict like with new config variables
        """
        # update with custom config
        self.config = stairs_config.ProjectConfig(**{**self.config, **config})
        self.init_apps()

    def init_apps(self) -> None:
        """
        Iterates over all apps in config and try to import them, with all
        components inside.

        At that stage we import and compile all stairs pipelines.
        """
        self.apps = []
        if self.config.get('apps', None):
            for app in self.config.apps:
                stairs_app.try_to_import(app)

        # compile pipelines and components here
        for app in self.apps:
            app.compile_components()

    def run_pipelines(self,
                      pipelines_to_run: List[AppPipeline] = None,
                      die_when_empty: bool = False,
                      die_on_error: bool = True) -> None:
        """
        Iterates by streaming queues and listening for a jobs related to
        defined pipelines.

        Filter steps which not related to pipeline (e.g. StandAloneConsumer)

        :param pipelines_to_run: list of Stairs pipelines which are
        listening for a jobs from streaming services. If not defined run all
        pipelines defined in stairs project.

        :param die_when_empty: If True - function return when no jobs found in
        streaming services.

        :param die_on_error: If True - function return when error happened.
        """

        steps_to_run = []
        for p in pipelines_to_run or []:
            steps_to_run.extend(p.get_workers_steps())

        # if custom pipelines defined, run all stairs pipelines.
        steps_to_run = steps_to_run or self.steps_to_run()

        # Filter steps which should not be run simultaneously with pipeline
        steps_to_run = [step for step in steps_to_run
                        if utils.is_step_related_to_pipelines(step)]

        self.stepist_app.run(steps_to_run,
                             die_on_error=die_on_error,
                             die_when_empty=die_when_empty)

    def get_app(self, name: str) -> StairsApp:
        """
        Generic function to get stairs app.
        """
        return self.get_app_by_name(name)

    def add_app(self, app: StairsApp) -> None:
        """
        Register new app in the project.
        """
        self.apps.append(app)

        for app in self.apps:
            app.compile_components()

    def add_job(self, pipeline_name: str, **data) -> None:
        """
        Add a job to streaming service which then will be forwarded to one
        of the worker (StairsProject process which executes run_pipelines
        method).

        :param pipeline_name: name of pipelines which will handle data. name could
        be defined as `app_name.pipeline_name` if `app_name` not defined it will
        search globally and in case when there are multiple pipelines with some
        name or no pipelines at all - raise RuntimeError.

        :param data: dict like data which will be forwarded to pipelines
        through streaming service.

        """
        pipeline = self.get_pipeline_by_name(pipeline_name)
        if pipeline is None:
            raise RuntimeError("Pipeline not found")

        pipeline.add_job(data)

    def get_app_by_name(self, name: str) -> StairsApp:
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
        """
        Search for stairs components which beehives like a workers. All of them
        defined in stepist app, but here we should extract only Stairs
        pipelines and pipelines components.

        Each Stairs component beehives like stepist.step object. It can play
        a worker role (listen streaming service) or a regular function, but
        all of them connected in one chain.

        Here we need only steps which defined/beehives as a workers.

        :return: list of stepist components to run
        """
        components_to_run = []

        for step in self.stepist_app.get_workers_steps():

            if utils.is_step_related_to_pipelines(step):
                components_to_run.append(step)

        return components_to_run

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

    def set_verbose(self, verbose) -> None:
        self.verbose = verbose
        self.stepist_app.set_verbose(verbose)

    def print(self, *args):
        if self.verbose:
            print(*args)

    @classmethod
    def init_from_file(cls, config_path: str, **init_kwargs):
        config = stairs_config.ProjectConfig.load_from_file(config_path)
        return cls(**init_kwargs, **config)
