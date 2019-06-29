import inspect

from stepist.flow.steps.next_step import call_next_step

from stairs.core.session.project_session import get_project

from stairs.core.utils import signals

from stairs.core import app_components
from stairs.core.worker import data_pipeline


class PipelineInfo:
    """
    User interface for pipeline.

    Pipeline itself stored as `pipeline_core`.
    """
    def __init__(self, pipeline_core, config=None):
        self.pipeline_core = pipeline_core
        self.app = pipeline_core.app
        self.config = config

    def key(self):
        return self.pipeline_core.key()


class Pipeline(app_components.AppPipeline):
    """
    Pipeline object is a controller which handle pipeline compilation, executing
    functions inside pipeline, and store configuration.

    Pipeline based on Stairs app (and using it for define streaming queues).

    Pipeline has a separate stepist step which always defined as "worker", and
    communicates with other parts of the system through streaming service.

    You can run pipeline using __call__ function - in this case pipeline
    will be executed directly without "streaming" service.

    If you want to run pipeline which will handle jobs from streaming service,
    it's recommended to do that from project context:

        from stairs import get_project
        get_project().run_pipelines(pipeline)

    Or using `run_stepist_worker` which just a shortcuts for that ^


    """
    def __init__(self,
                 app,
                 pipeline_builder,
                 worker_config):
        """
        :param app: Stairs app

        :param pipeline_builder: function which will handle pipeline building,
        first argument of this function, should be reserved for pipeline object

        :param worker_config: dict list object, used for pipeline configuration
        """
        self.app = app
        self.pipeline_builder = pipeline_builder

        self.config = worker_config

        # None means - not compiled yet.
        # call self.compile to build pipeline and generate stepist graph
        self.pipeline = None

        # stepist worker step, which execute __call__ when some job found in
        # streaming service
        self.step = self.app\
            .project\
            .stepist_app\
            .step(None,
                  as_worker=True,
                  unique_id=self.get_handler_name())(self)

        app_components.AppPipeline.__init__(self, self.app)

    def __call__(self, **kwargs) -> None:
        """
        Call first step of pipeline with kwargs as input data.

        It's important to compile pipeline before calling it.

        :param kwargs: input data for pipeline

        :return: Result of pipeline used only for testing purpose. And
        everything configure in a special way for that. In all other cases
        result will be None.
        """
        if not self.pipeline:
            raise RuntimeError("Seems like pipeline not build yet, call "
                               "Pipeline.compile() first")
        if not self.pipeline.is_compiled():
            raise RuntimeError("Worker pipeline no compiled, "
                               "run worker.compile()")

        result = call_next_step(kwargs, self.pipeline.get_stepist_root())

        if result:
            # only for testing. Not supported for production, yet.
            return list(result.values())[0]

    def run_stepist_worker(self,
                           die_on_error=True,
                           die_when_empty=False) -> None:
        """
        Run stepist app which listening streaming service and generating
        jobs for pipeline functions.

        It based on components which marked "as_worker" in current pipeline.
        See self.get_workers_steps for more details

        :param die_on_error: If True - exit on error
        :param die_when_empty: If True - exit when streaming service empty
        """
        steps_to_run = self.get_workers_steps()

        get_project().stepist_app.run(steps_to_run,
                                      die_on_error=die_on_error,
                                      die_when_empty=die_when_empty)

    def compile(self) -> None:
        """
        Build data pipeline based on user function into stepist graph.

        On this stage all user functions converted to stepist objects (steps)
        and connected into one chain.

        Compile function can be executed multiple times, take this into account
        when using `on_pipeline_ready` signal.
        """
        self.pipeline = self.make_pipeline(self.config)
        self.pipeline.compile()

        signals.on_pipeline_ready(self).send_signal(self)

    def make_pipeline(self, config=None) -> data_pipeline.DataPipeline:
        """
        Execute pipeline builder (user function which makes pipeline graph).

        As a result we have a set of stairs components connected into one
        chain using stepist app. And result of one step is forwarded as an
        input to another.

        On this stage we populate `pipeline builder` function with a set
        of DataPoints (stairs objects which you can use to build data pipeline).
        Then build stepist graph and at the final stage ensure that everything
        specified and result data is valid.

        :param config: custom config for pipeline
        :return: DataPipeline object
        """
        if config is None:
            # TODO: probably we need to update self.config by passed config
            config = self.config

        worker_info = PipelineInfo(self, config=config)

        initial_pipeline = data_pipeline.DataPipeline.make_empty(self.app,
                                                                 worker_info)
        initial_frames = self.initial_data_frames(initial_pipeline)

        worker_data_frame = self.pipeline_builder(worker_info, **initial_frames)
        ensure_worker_output_is_valid(worker_data_frame)

        return worker_data_frame.data_pipeline

    def key(self):
        return self.get_handler_name()

    def get_worker_input_values(self) -> list:
        """
        Return variables which user expect to get inside pipeline
        :return:
        """
        info = inspect.getfullargspec(self.pipeline_builder)
        args = info[0]
        # exclude worker var
        args.pop(0)
        return args

    def initial_data_frames(self, pipeline):
        worker_input_keys = self.get_worker_input_values()

        initial_frames = dict()
        for key in worker_input_keys:
            data_point = data_pipeline.DataPoint(pipeline.deepcopy(), {key: key})
            initial_frames[key] = data_point

        return initial_frames

    def get_workers_steps(self):
        """
        Extract components which are behaves as workers.

        Including steps which should be run simultaneously with workers (such
        as StandAloneConsumer)

        :return: All possible steps which markers `as_worker` inside pipeline
        """
        workers_steps = [self.step]

        if not self.pipeline:
            raise RuntimeError("Pipeline is not compiled")

        for step in self.pipeline.stepist_steps.values():
            if step.as_worker:
                workers_steps.append(step)

        return workers_steps

    def add_job(self, data):
        call_next_step(data, self.step)

    def get_queue_name(self):
        return self.step.get_queue_name()

    def get_stepist_step(self):
        return self.step

    def flush_queue(self):
        # TODO: flush all jobs in stepist
        pass

    def get_handler_name(self):
        return self.pipeline_builder.__name__

    def __name__(self):
        module_name = inspect.getmodule(self.pipeline_builder).__name__
        return "%s:%s:%s" % (self.app.app_name,
                             module_name, self.get_handler_name())


def ensure_worker_output_is_valid(w_output):

    # if len(list(w_output)) != 1:
    #     raise RuntimeError("Worker should return one DataFrame")

    if w_output is None:
        raise RuntimeError("Worker output can't be None")
