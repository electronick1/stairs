from stepist.flow.workers.adapters import simple_queue, rm_queue, sqs_queue

from stairs.services import spark as spark_workers
from stairs.core.app import components


class SparkProducer(components.AppProducer):
    """

    """
    DEFAULT_QUEUE_LIMIT = 10 ** 6

    def __init__(self, app, handler, default_callbacks: list,
                 custom_callbacks: dict, queue_limit=None):

        self.app = app

        self.queue_limit = queue_limit or self.DEFAULT_QUEUE_LIMIT

        # The main generator which yields data
        self.handler = handler

        # Callbacks which should be run always
        self.default_callbacks = default_callbacks or []
        # Callbacks which should be run based on user console, input
        self.custom_callbacks = custom_callbacks

        components.AppProducer.__init__(self, app)

    def __call__(self, *args, **kwargs):
        self.run(user_args=args, user_kwargs=kwargs)

    def run(self, custom_callbacks_keys: list = None,
            user_args=None, user_kwargs=None):
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

        user_args = user_args or []
        user_kwargs = user_kwargs or dict()

        worker_engine = self.app.project.stepist_app.worker_engine

        if isinstance(worker_engine, simple_queue.SimpleQueueAdapter):
            spark_worker = spark_workers\
                .redis_queue\
                .get_connection(worker_engine)
        elif isinstance(worker_engine, rm_queue.RQAdapter):
            spark_worker = spark_workers \
                .rm_queue \
                .get_connection(worker_engine)
        elif isinstance(worker_engine, sqs_queue.SQSAdapter):
            spark_worker = spark_workers \
                .sqs_queue \
                .get_connection(worker_engine)
        else:
            raise RuntimeError("Spark don't support current queue broken")

        steps_keys_to_run = [c.step.step_key() for c in callbacks_to_run]
        # Running jobs from producer
        spark_rdd = self.handler(*user_args, **user_kwargs)

        SparkJobs(spark_worker, steps_keys_to_run).show_must_go_on(spark_rdd)

    def get_producer_id(self):
        return "producer:%s:%s" % (self.app.app_name, self.handler.__name__)

    def get_handler_name(self):
        return self.handler.__name__

    def key(self):
        return self.get_handler_name()


class SparkJobs:
    def __init__(self, spark_worker, steps_keys):
        self.spark_worker = spark_worker
        self.steps_keys = steps_keys

    def show_must_go_on(self, spark_rdd):
        spark_rdd.foreachPartition(self.handle_rdd)

    def handle_rdd(self, rdd):
        self.spark_worker.init_connection()
        for item in rdd:
            self(item.asDict())

    def __call__(self, row_data):
        for key in self.steps_keys:
            self.spark_worker.add_job(key, row_data)
