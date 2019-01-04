import inspect

from stepist.flow.steps.next_step import call_next_step

from stairs.core.app import components
from stairs.core.worker import data_pipeline


class WorkerInfo:

    def __init__(self, base_worker, config=None):
        self.base_worker = base_worker
        self.app = base_worker.app
        self.config = config

    def key(self):
        return self.base_worker.key()


class Worker(components.AppWorker):
    def __init__(self, app, pipeline_builder, worker_config):
        self.pipeline_builder = pipeline_builder
        self.app = app

        self.config = worker_config
        self.pipeline = self.make_pipeline(worker_config)
        self.compile()

        self.step = self.app\
            .project\
            .stepist_app\
            .step(None,
                  as_worker=True,
                  unique_id=self.get_handler_name())(self)

        components.AppWorker.__init__(self, self.app)

    def __call__(self, **kwargs):
        if not self.pipeline.is_compiled():
            raise RuntimeError("Worker pipeline no compiled, "
                               "run worker.compile()")

        result = call_next_step(kwargs, self.pipeline.get_stepist_root())

        if result:
            return list(result.values())[0]

    def compile(self):
        self.pipeline.compile()

    def make_pipeline(self, config=None):
        worker_info = WorkerInfo(self, config=config)

        initial_pipeline = data_pipeline.DataPipeline.make_empty(self.app,
                                                                 worker_info)
        initial_frames = self.initial_data_frames(initial_pipeline)

        worker_data_frame = self.pipeline_builder(worker_info, **initial_frames)
        ensure_worker_output_is_valid(worker_data_frame)

        return worker_data_frame.data_pipeline

    def key(self):
        return self.get_handler_name()

    def get_worker_input_values(self):
        info = inspect.getfullargspec(self.pipeline_builder)
        args = info[0]
        # exclude worker var
        args.pop(0)
        return args

    def initial_data_frames(self, pipeline):
        worker_input_keys = self.get_worker_input_values()

        initial_frames = dict()
        for key in worker_input_keys:
            data_point = data_pipeline.DataPoint(pipeline, {key: key})
            initial_frames[key] = data_point

        return initial_frames

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
