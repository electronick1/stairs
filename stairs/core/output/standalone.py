from stairs.core.session import project_session

from stairs.core.output.output_model import Output


class StandAloneConsumer(Output):

    def __init__(self, *args, **kwargs):
        Output.__init__(self, *args, **kwargs, as_daemon=True)

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    def run_worker(self):
        stairs_project = project_session.get_project()
        worker_engine = stairs_project.stepist_app.worker_engine

        worker_engine.process(self.step)
