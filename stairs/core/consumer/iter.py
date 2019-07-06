import time

from stepist.flow.utils import validate_handler_data
from stepist.flow.steps.next_step import call_next_step

from stairs.core.session.project_session import get_project

from stairs.core.consumer import Consumer
from stairs.core import app_components


class ConsumerIter(Consumer):

    def __init__(self, app, handler):
        self.handler = handler
        self.app = app

        self.step = self.app \
            .project \
            .stepist_app \
            .step(None, name=self.key(), as_worker=True)(self.run_job)

        app_components.AppConsumer.__init__(self, self.app)

    def __call__(self, **kwargs):
        handler_data = validate_handler_data(self.handler, kwargs)

        result = self.handler(**handler_data)
        call_next_step(result, self.step)

    def iter(self, die_when_empty=False):
        """
        User interface for jobs_iterator.
        """
        for data in self.jobs_iterator(die_when_empty=die_when_empty):
            yield data

    def jobs_iterator(self, die_when_empty=False):
        """
        The way how it works a bit tricky.
        We manually grab data from worker engine, but we can't just return
        it because stepist session is not initialized.
        For this reason we manually executing step and then just return
        result of this step (result of run_job)
        """
        project = get_project()

        while True:
            job = project.stepist_app.worker_engine.receive_job(self.step)
            if job is None:
                if die_when_empty:
                    return
                project.print("No jobs, waiting ... ")
                time.sleep(3)
                continue

            yield job.get('flow_data', None)

    def run_job(self, **job_data):
        return job_data

    def key(self):
        return "stairs::consumer_iter::%s" % self.handler.__name__
