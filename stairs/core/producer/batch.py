import time
from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.session.project_session import get_project

from stairs.core.producer.utils import producer_retry
from stairs.core import app_components


class BatchProducer(app_components.AppProducer):

    retry_sleep_time = 5

    def __init__(self, app, handler, simple_producer,
                 repeat_on_signal=None, repeat_times=None):
        self.app = app

        self.repeat_on_signal = repeat_on_signal
        self.repeat_times = repeat_times

        # The main generator which yields data
        self.handler = handler
        self.producer = simple_producer

        app_components.AppProducer.__init__(self, app)

    def __call__(self, *args, **kwargs):
        if self.repeat_on_signal:
            self.run_signal_repeat(**kwargs)
        elif self.repeat_times:
            for _ in range(self.repeat_times):
                self.run(**kwargs)
        else:
            self.run(**kwargs)

    def run(self, *args, **kwargs):
        self.send_jobs(self.handler(*args, **kwargs))

    def run_signal_repeat(self, **user_kwargs):
        repeat_count = 0

        while True:
            self.run(**user_kwargs)

            if self.repeat_on_signal is None:
                return

            get_project().print("Finish %s iteration for batch generation. "
                                "Run producer:run_jobs to execute producer"
                                % repeat_count)

            while not self.repeat_on_signal(self.producer):
                time.sleep(self.retry_sleep_time)

            repeat_count += 1
            if self.repeat_times and repeat_count > self.repeat_times:
                get_project().print("Producer successfully repeated %s times" % (
                                    repeat_count - 1))
                break

            get_project().print("Repeating batch producer ..." )

    def flush(self):
        self.producer.stepist_step.flush_all()

    def send_jobs(self, job_iterator):
        with session.change_flow_ctx({}, {}):
            for job in job_iterator:
                # TODO: Make it more safe using single transaction
                try:
                    self.send_job_to_producer(job)
                except Exception as e:
                    print("Something happened during batch generation, producer"
                          " was interrupted and jobs had forwarded to queue")
                    print("Run producer:flush for `%s` producer "
                          % self.producer.get_handler_name())
                    raise

    @producer_retry(5, Exception)
    def send_job_to_producer(self, stepist_job):
        call_next_step(stepist_job, self.producer.stepist_step)

    def key(self):
        return self.handler.__name__
