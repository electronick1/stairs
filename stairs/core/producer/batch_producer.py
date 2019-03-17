from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.producer.utils import producer_retry
from stairs.core.app import components


class BatchProducer(components.AppProducer):
    DEFAULT_QUEUE_LIMIT = 10 ** 9

    def __init__(self, app, handler, simple_producer, queue_limit=None):
        self.app = app

        self.queue_limit = queue_limit or self.DEFAULT_QUEUE_LIMIT

        # The main generator which yields data
        self.handler = handler
        self.producer = simple_producer

        components.AppProducer.__init__(self, app)

    def __call__(self, *args, **kwargs):
        jobs_iterator = self.handler(*args, **kwargs)
        self.send_jobs(jobs_iterator)

    def flush(self):
        self.producer.stepist_step.flush_all()

    def send_jobs(self, job_iterator):
        with session.change_flow_ctx({}, {}):
            for job in job_iterator:
                # TODO: Make it more safe
                try:
                    self.send_job_to_producer(job)
                except Exception as e:
                    print("Something happened during batch generation, producer"
                          " was interrupted and jobs had forwarded to queue")
                    print("Run producer:flush for `%s` producer "
                          % self.producer.get_handler_name())
                    raise(e)

    @producer_retry(5, Exception)
    def send_job_to_producer(self, stepist_job):
        call_next_step(stepist_job, self.producer.stepist_step)

    def key(self):
        return self.handler.__name__
