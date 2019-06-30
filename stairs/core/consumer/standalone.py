from stepist.flow.utils import validate_handler_data

from stairs.core.session import project_session

from stairs.core.consumer import Consumer


class StandAloneConsumer(Consumer):

    def __init__(self, *args, **kwargs):
        Consumer.__init__(self, *args, **kwargs, as_worker=True)

    def __call__(self, *args, **kwargs):
        kwargs = validate_handler_data(self.handler, kwargs)
        return self.handler(*args, **kwargs)

    def run_worker(self):
        stairs_project = project_session.get_project()
        worker_engine = stairs_project.stepist_app.worker_engine

        worker_engine.process(self.step)
