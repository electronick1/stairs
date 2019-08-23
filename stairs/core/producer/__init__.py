import time

from functools import wraps
from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.session.project_session import get_project

from stairs.core import app_components
from stairs.core.producer.utils import producer_retry


class Producer(app_components.AppProducer):
    """

    """
    retry_sleep_time = 5

    def __init__(self, app, handler, default_callbacks: list,
                 single_transaction=False, repeat_on_signal=None,
                 repeat_times=None):

        self.app = app

        self.single_transaction = single_transaction

        # The main generator which yields data
        self.handler = handler

        # Callbacks which should be run always
        self.default_callbacks = default_callbacks or []

        # Restart producer by function signal
        self.repeat_on_signal = repeat_on_signal
        # Amount of times when we need to retry producer, also useful with
        # signal
        self.repeat_times = repeat_times

        # Stepist step basically to forward jobs to current producer
        # e.g. from Batch Producer
        self.stepist_step = self.app\
            .project\
            .stepist_app\
            .step(None,
                  as_worker=True,
                  unique_id=self.get_producer_id())(self.run)

        app_components.AppProducer.__init__(self, app)

    def __call__(self, **kwargs):

        if self.default_callbacks:
            get_project().print("Generating jobs for: ")
            for callback in self.default_callbacks:
                get_project().print("-> %s" % callback.get_handler_name())
        else:
            get_project().print("No callbacks found, running empty producer ..")

        if self.repeat_on_signal:
            self.run_signal_repeat(**kwargs)
        elif self.repeat_times:
            for _ in range(self.repeat_times):
                self.run(**kwargs)
        else:
            self.run(**kwargs)

    def run(self, **user_kwargs):
        """
        Execute producer from console with specified args and kwargs.
        Also can have custom callbacks specified there.
        """

        callbacks_to_run = self.default_callbacks

        single_transaction = self.single_transaction
        user_kwargs = user_kwargs or dict()

        # Running jobs from producer
        if not single_transaction:
            for job in self.handler(**user_kwargs):
                self.send_job(job, callbacks_to_run)
        else:
            jobs_to_send = list(self.handler(**user_kwargs))
            self.send_jobs(jobs_to_send, callbacks_to_run)

    def run_signal_repeat(self, **user_kwargs):
        repeat_count = 0

        while True:
            self.run(**user_kwargs)
            if self.repeat_on_signal is None:
                return

            get_project().print("Finish %s iteration, waiting signal"
                                % (repeat_count + 1))

            while not self.repeat_on_signal(self):
                time.sleep(self.retry_sleep_time)

            repeat_count += 1
            if self.repeat_times and repeat_count > self.repeat_times:
                get_project().print("Producer successfully repeated %s times"
                                    % (repeat_count-1))
                break

            get_project().print("Got signal `%s`, repeating .." %
                                self.repeat_on_signal.__name__)

    def run_jobs(self, die_when_empty=False):
        run_jobs_processor(project=get_project(),
                           producers_to_run=[self],
                           die_when_empty=die_when_empty)

    def send_job(self, job, callbacks_to_run):
        with session.change_flow_ctx({}, {}):

            # TODO: Make it more safe, it will good to run all callbacks
            # in one transaction, otherwise there is a small chance to fail some
            # callback and duplication/lose data
            for callback in callbacks_to_run:
                self._job_to_stepist(job, callback.step)

    def send_jobs(self, jobs, callbacks_to_run):
        with session.change_flow_ctx({}, {}):

            # TODO: Make it more safe, it will good to run all callbacks
            # in one transaction, otherwise there is a chance to fail some
            # callback and duplication/lose data
            for callback in callbacks_to_run:
                self._job_to_stepist(jobs, callback.step, batch_data=True)

    @producer_retry(5, Exception)
    def _job_to_stepist(self, stepist_job, step, **kwargs):
        call_next_step(stepist_job, step, **kwargs)

    def redirect_handler(self, handler):
        producer_chain = lambda *args, **kwargs: handler(self.handler(*args,
                                                                      **kwargs))

        return wraps(handler)(producer_chain)

    def get_producer_id(self):
        return "producer:%s:%s" % (self.app.app_name, self.handler.__name__)

    def get_handler_name(self):
        return self.handler.__name__

    def get_stepist_step(self):
        return self.stepist_step

    def flush(self):
        for pipeline in self.default_callbacks:
            pipeline.step.flush_all()

    def key(self):
        return self.get_handler_name()


def run_jobs_processor(project, producers_to_run, die_when_empty=False):
    """
    Executing forwarded jobs (from batch producer)
    """

    steps_to_run = [p.stepist_step for p in producers_to_run]

    project \
       .stepist_app\
       .run(steps_to_run,
            die_on_error=True,
            die_when_empty=die_when_empty)
