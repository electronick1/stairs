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
        """
        The way how it works a bit tricky.
        We manually grab data from worker engine, but we can't just return
        it because stepist session is not initialized.
        For this reason we manually executing step and then just return
        result of this step (result of run_job)
        """
        project = project_session.get_project()

        while True:
            job = project.stepist_app.worker_engine.receive_job(self.step)
            data = self.step.receive_job(**job)
            # it's last step in step and it will be dict with "run_job" key
            yield data.get("run_job")

    def run_job(self, **job_data):
        return job_data
