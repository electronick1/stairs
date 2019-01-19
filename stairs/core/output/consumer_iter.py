from stairs.core.session import project_session

from stairs.core.output.output_model import Output
from stairs.core.app import components


class ConsumerIter(Output):

    def __init__(self, app, handler, as_daemon=False):
        self.handler = handler
        self.app = app

        self.step = self.app \
            .project \
            .stepist_app \
            .step(None, as_worker=True)(self.run_job)

        components.AppOutput.__init__(self, self.app)

    def __call__(self, *args, **kwargs):
        for data in self.jobs_iterator():
            yield self.handler(**data)

    def jobs_iterator(self):
        project = project_session.get_project()

        while True:
            yield project.stepist_app.worker_engine.receive_job(self.step)

    def run_job(self):
        raise RuntimeError("Consumer iter can't process jobs as worker")
