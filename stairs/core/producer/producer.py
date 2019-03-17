from stepist.flow import session
from stepist.flow.steps.next_step import call_next_step

from stairs.core.session import producer_session
from stairs.core.app import components
from stairs.core.producer.utils import producer_retry, custom_callbacks_to_dict


class Producer(components.AppProducer):
    """

    """
    DEFAULT_QUEUE_LIMIT = 10 ** 6

    def __init__(self, app, handler, default_callbacks: list,
                 custom_callbacks: list, queue_limit=None,
                 single_transaction=False):

        self.app = app

        self.queue_limit = queue_limit or self.DEFAULT_QUEUE_LIMIT
        self.single_transaction = single_transaction

        # The main generator which yields data
        self.handler = handler

        # Callbacks which should be run always
        self.default_callbacks = default_callbacks or []
        # Callbacks which should be run based on user console, input
        self.custom_callbacks = custom_callbacks_to_dict(custom_callbacks or [])

        # Stepist step basically to forward jobs to current producer
        # e.g. from Batch Producer
        self.stepist_step = self.app\
            .project\
            .stepist_app\
            .step(None,
                  as_worker=True,
                  unique_id=self.get_producer_id())(self.run_jobs)

        components.AppProducer.__init__(self, app)

    def __call__(self, *args, **kwargs):
        self.run(user_args=args, user_kwargs=kwargs)

    def run(self, custom_callbacks_keys: list = None,
            single_transaction: bool = False, user_args=None, user_kwargs=None):
        """
        Execute producer from console with specified args and kwargs.
        Also can have custom callbacks specified there.
        """
        custom_callbacks = []

        # Basic check for custom producers
        for custom_callback in custom_callbacks_keys or []:
            callback = self.custom_callbacks.get(custom_callback, None)
            if callback is None:
                print("Producer callback `%s` (another producer or pipeline"
                      ") not found." % custom_callback)
                exit()
            custom_callbacks.append(callback)

        # Basic check for callbacks
        if not custom_callbacks and not self.default_callbacks:
            print("No callbacks was found, specified default callback or use"
                  "custom callback")
            exit()

        callbacks_to_run = custom_callbacks + self.default_callbacks

        single_transaction = single_transaction or self.single_transaction
        user_args = user_args or []
        user_kwargs = user_kwargs or dict()

        # Running jobs from producer
        if not single_transaction:
            for job in self.handler(*user_args, **user_kwargs):
                self.send_job(job, callbacks_to_run)
        else:
            jobs_to_send = list(self.handler(*user_args, **user_kwargs))
            self.send_jobs(jobs_to_send, callbacks_to_run)

    def run_jobs(self, **kwargs):
        """
        Stepist Handler for executing forwarded jobs.

        Important(!) If you want to use custom callbacks, set them using:
        `producer_session.change_custom_callbacks` contextmanager
        """
        custom_callbacks = producer_session.get_custom_callbacks()
        self.run(custom_callbacks_keys=custom_callbacks,
                 user_kwargs=kwargs,
                 single_transaction=True)

    def send_job(self, job, callbacks_to_run):
        with session.change_flow_ctx({}, {}):

            # TODO: Make it more safe, it will good to run all callbacks
            # in one transaction, otherwise there is a chance to fail some
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

    def get_producer_id(self):
        return "producer:%s:%s" % (self.app.app_name, self.handler.__name__)

    def get_handler_name(self):
        return self.handler.__name__

    def get_stepist_step(self):
        return self.stepist_step

    def flush(self):
        for pipeline in self.default_callbacks:
            pipeline.step.flush_all()

        for pipeline in self.custom_callbacks.values():
            pipeline.step.flush_all()

    def key(self):
        return self.get_handler_name()


def run_jobs_processor(project, producers_to_run, custom_callbacks_keys: list = None,
                       die_when_empty=False):
    """
    Executing forwarded jobs (from batch producer)
    """
    steps_to_run = [p.stepist_step for p in producers_to_run]
    with producer_session.change_custom_callbacks(custom_callbacks_keys):
        project \
           .stepist_app\
           .run(steps_to_run,
                die_on_error=True,
                die_when_empty=die_when_empty)