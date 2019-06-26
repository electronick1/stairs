import time

from stepist.flow.utils import validate_handler_data

from stairs.core.session import project_session

from stairs.core.consumer.consumer_model import Consumer
from stairs.core.app import components


class ConsumerIter(Consumer):

    def __init__(self, app, handler):
        self.handler = handler
        self.app = app

        self.step = self.app \
            .project \
            .stepist_app \
            .step(None, name=self.key(), as_worker=True)(self.run_job)

        components.AppConsumer.__init__(self, self.app)

    def __call__(self, *args, **kwargs):
        for data in self.jobs_iterator():
            data = validate_handler_data(self.handler, data)
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
            if job is None:
                print("No jobs, waiting ... ")
                time.sleep(3)
                continue
            data = self.step.receive_job(**job)
            # it's last step in step and it will be dict with "run_job" key
            yield data.get("run_job")

    def run_job(self, **job_data):
        return job_data

    def key(self):
        return "stairs::consumer_iter::%s" % self.handler.__name__
