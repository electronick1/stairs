from typing import Union
from stairs.core.producer.producer import Producer
from stairs.core.producer.batch_producer import BatchProducer
from stairs.core.consumer.consumer_model import Consumer
from stairs.core.consumer.standalone import StandAloneConsumer
from stairs.core.consumer.consumer_iter import ConsumerIter
from stairs.core.worker.pipeline_model import Pipeline


class ComponentsMixin:
    """
    User interface and initialization of all app components. Its a part of
    stairs.App class.

    It's a facade which implement decorator like functions to setup and configure
    different stairs app components.

    Also implements some shortcuts for app.
    For example getting basic components, such like `get_pipeline`.
    """

    # App components aggregator
    components = None

    def producer(self,
                 *pipelines: Pipeline,
                 single_transaction=False):
        """
        Creates Stairs producer component.

        Producer allows you to populate your pipeline with data in a very
        simple way. Producer function should just return/yield some data which
        then will be forwarded to streaming service and picked up by workers
        (pipeline process).

        (!)A result of your producer function should be always dict like object.

        Usage as decorator:

            @producer(my_pipeline)
            def producer_function():
                yield dict()

        As a function:

            def producer_function2():
                yield dict()

            p = producer(my_pipeline)(producer_function2)

        You can run producer from stairs cli:

            python manage.py producer:run

        Or just by calling Producer instance:

            @producer(my_pipeline)
            def producer_function():
                yield dict()

            producer_function()

        It's also possible to define arguments for producer:

            @producer(my_pipeline)
            def producer_function(message):
                yield dict(msg=message)

            producer_function("Hello world")


        `single_transaction` allows you to commit all data which were
        return/yield by producer function to streaming service, at once. It
        useful when you need to achieve high fault tolerance, but it could be a
        problem for memory and network limits.

        :param pipelines: list of Stairs pipelines
        :param single_transaction: True - if you want to commit data which were
        yield in one transaction

        :return: function wrapper which returns Producer
        """
        def _producer_handler_wrap(handler) -> Producer:
            producer = Producer(app=self,
                                handler=handler,
                                default_callbacks=list(pipelines),
                                single_transaction=single_transaction)

            return producer

        return _producer_handler_wrap

    def batch_producer(self, producer: Producer) -> BatchProducer:
        """
        Next iteration for Producer.
        Batch producer allows you to generate jobs for regular producer. It's
        like distributed way to calling simple producer component.

        Batch producer return/yields data (which will be input for Producer),
        then this data goes to streaming service. And then you can forward jobs
        from streaming service to Producer.

        (one process)                          (another process)
        Batch producer -> streaming service -> Producer

        It allows you to run Producers in distribute parallel way, and it
        useful when you want to read smth (e.g. database) in async way.

            @producer(my_pipeline)
            def simple_producer(files):
                for file in files:
                    yeild dict(data=open(file, "r").read())

            @batch_producer(simple_producer)
            def my_batch_producer():
                for batch_of_files in all_files:
                    yeild dict(files=batch_of_files)

        To run this you can run batch producer as always:

            python manage.py producer:run my_batch_producer

        It will execute my_batch_producer, and populate streaming service
        with jobs. Then simple_producer execution will start automatically -
        it will read streaming service and populate pipeline.

        If you want to run more simple_producer "workers" you can use:

            python manage.py producer:run_jobs simple_producer

        It will start another process which will read streaming service
        and execute simple_producer

        You can do the same from python:

            # will populate streaming service
            my_batch_producer()

            # read streaming service, and stop execute which queue is empty
            simple_producer.run_jobs(die_when_empty=True)


        :param producer: Stairs producer instance
        :return: Stairs Batch producer instance
        """

        def _batch_producer_handler_wrap(handler):
            batch_producer = BatchProducer(app=self,
                                           handler=handler,
                                           simple_producer=producer)
            return batch_producer

        return _batch_producer_handler_wrap

    def producer_redirect(self,
                          based_on: Producer,
                          *pipelines: Pipeline):
        """
        The way to execute some producer with a different set of pipelines.

        It will simply run `based_on` producer, and forward result to current
        function, then you can run new custom pipelines.

        :param based_on: Stairs producer Instance which will executed before
        current function

        :param pipelines: List of Stairs pipelines
        :return: Stairs Producer instance with modified function.
        """

        def _producer_redirect_handler_wrap(handler) -> Producer:
            # wrapping current handler by `base_on` producer handler
            redirect_handler = based_on.redirect_handler(handler)

            producer = Producer(app=self,
                                handler=redirect_handler,
                                default_callbacks=list(pipelines),
                                single_transaction=based_on.single_transaction)

            return producer

        return _producer_redirect_handler_wrap

    def spark_producer(self, *pipelines: Pipeline):
        """
        Producer where you can use Spark RDD inside. Result of producer function
        should Spark RDD which then will be executing using `foreachPartition`
        method.

        It also support BatchProducer and you can operate with that in the
        same way like Stairs Producer component.

        :param pipelines: Stairs pipelines instances
        """
        from stairs.core.producer.spark_producer import SparkProducer

        def _spark_producer_handler_wrap(handler) -> Producer:
            producer = SparkProducer(app=self,
                                     handler=handler,
                                     default_callbacks=list(pipelines))

            return producer

        return _spark_producer_handler_wrap

    def pipeline(self, config=None):
        """
        Pipeline is a way to connect all functions and data handler into one
        execution flow.

        Each component of pipeline could be a `worker` which communicates with
        others parts through streaming service.

        (!) It's important to know that each pipeline behaves as worker, and
        communicates with other components through streaming service. It's
        not recommended to call pipeline directly (without streaming service).

        First parameter of your function should be `pipeline` object. You can
        use it for manipulating of config object or control different parts of
        pipeline inside.

        Example:

            @app.pipeline()
            def my_pipeline(pipeline, data):
                return data.subscribe_func(lambda x: dict(x=x))

        As you can see first argument of `my_pipeline` it's a current Pipeline
        instance, then you can specify all other variables which will represent
        your data as `stairs.DataPoint` objects.

        When you run pipeline, process will listening for a jobs from streaming
        service and run functions/steps defined inside pipeline.
        To do that you can use stairs cli:

            python manage.py pipelines:run

        Or you can run pipelines from project:

            from stairs import get_project
            get_project().run_pipelines(pipelines_to_run)

        Each pipeline store config object inside. It can be used to control
        pipeline outside, for example from another pipelines. Here you can
        define `default` config which will used if no other parameters specified.

        Config Example:

            @app.pipeline(config=dict(path='/'))
            def my_pipeline(pipeline, data):
                if pipeline.config.get('path') != '/':
                    return data.subscribe_func(lambda x: dict(x=x))
                else:
                    return data.subscribe_func(lambda x: dict(x=x))

        :param config: default config for pipeline
        :return:
        """

        def _pipeline_handler_wrap(func) -> Pipeline:
            return Pipeline(self, func, config)

        return _pipeline_handler_wrap

    def consumer(self):
        """
        It's a component which don't change pipeline data and behaves as
        a standalone function.

        Consumer useful in case when you need to save or load data somewhere.

        Consumer will be executed simultaneously with pipelines by cli command:

            python manage.py pipelines:run

        Or by execute pipelines directly:

            project.run_pipelines(pipelines_to_run)

        If you want to achieve true fault tolerance use consumer as a worker
        `as_worker=True` inside pipeline and make only one db transaction inside.

        """

        def _handler_wrap(func) -> Consumer:
            return Consumer(app=self, handler=func)

        return _handler_wrap

    def standalone_consumer(self) -> StandAloneConsumer:
        """
        Standalone consumer is the same component as a "app.consumer", but
        it will be executed only in a separate process.

        (one process)                                (another process)
        function in pipeline -> Streaming service -> standalone consumer

        Standalone consumer is always "worker", and to start executing jobs you
        need to run cli command:

            python manage.py consumer:standalone consumer_name

        or by calling a method:

            @app.standalone_consumer()
            def my_consumer():
                pass

            my_consumer.run_worker()

        """
        def _handler_wrap(func):
            return StandAloneConsumer(app=self, handler=func)

        return _handler_wrap

    def consumer_iter(self):
        """
        It's a standalone consumer (similar to `app.standalone_consumer`) but,
        it will yield all data back to the process which executed this cosumer.

        (one process)
        pipeline -> Streaming service ->
           (               another process                 )
        -> consumer_iter -> process which call consumer_iter

        Consumer iter will be executed only when it's called as a generator:

            @app.consumer_iter():
            def my_consumer(value):
                return value

            for v in my_consumer():
                print(v)

        In example above, when you call my_consumer() it will return a generator
        which will forward job from streaming service to `my_consumer` and then
        yield result of `my_consumer` back to the circle.

        """
        def _handler_wrap(func) -> ConsumerIter:
            return ConsumerIter(app=self, handler=func)

        return _handler_wrap

    def get_pipeline(self, name) -> Pipeline:
        return self.components.pipelines.get(name)

    def get_producer(self, name) -> Union[Producer, BatchProducer]:
        return self.components.producers.get(name)

    def get_consumer(self, name) -> \
            Union[ConsumerIter, StandAloneConsumer, Consumer]:
        return self.components.consumers.get(name)
