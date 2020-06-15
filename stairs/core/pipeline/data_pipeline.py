import copy
import uuid
from typing import Union
from stepist.flow.steps.next_step import call_next_step as stepist_next_step
from stepist.flow.utils import validate_handler_data

from stairs.core.pipeline.pipeline_objects import (PipelineFlow,
                                                   PipelineFlowProducer,
                                                   PipelineOutput,
                                                   PipelineFunction,
                                                   PipelineConnectorComponent,
                                                   PipelineFunctionProducer)

from stairs.core.flow import Flow
from stairs.core.consumer import Consumer
from stairs.core.consumer.standalone import StandAloneConsumer
from stairs.core.consumer.iter import ConsumerIter

from stairs.core.pipeline import pipeline_graph
from stairs.core.pipeline.pipeline_graph import concatenate_sequentially

from stairs.core.pipeline.pipeline_objects import \
    transformation as transformations_types


class DataPipeline:
    """

    """

    def __init__(self, app, worker_info, graph=None, initial_component=None):
        self.app = app
        self.worker_info = worker_info

        # if graph not define, trying to make "empty" graph
        if graph is None:
            graph = pipeline_graph.PipelineGraph()

            if initial_component is None:
                initial_component = self.get_initial_component(worker_info)

            graph.add_pipeline_component(initial_component)

        self.graph = graph
        self.stepist_steps = dict()

    def __call__(self, *args, **kwargs):
        if not self.stepist_steps:
            raise RuntimeError("Pipeline not compiled")

        stepist_next_step(self.get_stepist_root(), **kwargs)

    def deepcopy(self):
        cls = self.__class__

        return cls(self.app,
                   worker_info=self.worker_info,
                   graph=self.graph.deepcopy())

    def keys(self):
        if not self.graph.is_line_tree():
            raise RuntimeError("Can't get keys from graph with multiple leaves")

        leave = pipeline_graph.get_leaves(self.graph)[0]
        return leave.p_component.get_keys()

    def add_pipeline_component(self, p_component, transformation:dict):
        graph_items_affected = self.graph.add_pipeline_component(p_component)
        for g_item in graph_items_affected:
            g_item.p_component.add_context(p_component,
                                           transformations_types.KeyToKey(transformation))

        return self

    def add_pipeline_graph(self, graph, transformation,
                           forward_data_to_leaves=False):
        graph_items_affected = self.graph.add_graph_on_leaves(graph)

        root_p_component = graph.get_root().p_component

        for g_item in graph_items_affected:
            g_item.p_component.add_context(
                root_p_component,
                transformations_types.KeyToKey(transformation)
            )

        if forward_data_to_leaves:
            for g_item in pipeline_graph.get_leaves(graph):
                root_p_component.add_context(
                    g_item.p_component,
                    transformations_types.KeyToKey(transformation)
                )

    def get_last_item(self):
        return pipeline_graph.get_leaves(self.graph)[0]

    def compile(self):
        if self.is_compiled():
            raise RuntimeError("Pipeline already compiled")

        self.stepist_steps = compile_pipeline(self)

    def is_compiled(self):
        return bool(self.stepist_steps)

    def get_stepist_id(self, component_id):
        return "%s:%s" % (self.worker_info.key(), component_id)

    # utils:
    def get_stepist_root(self):
        root_component = self.graph.get_root().p_component
        stepist_id = self.get_stepist_id(root_component.id)

        return self.stepist_steps[stepist_id]

    @classmethod
    def make_empty(cls, app, worker_info):
        return cls(
            app,
            worker_info,
            initial_component=cls.get_initial_component(worker_info)
        )

    @classmethod
    def get_initial_component(cls, worker_info):
        return PipelineConnectorComponent(
            pipeline=None,
            name="%s:root" % worker_info.key()
        )


class ConditionPipeline:
    def __init__(self, statement, do_pipeline, otherwise_pipeline=None):
        self.statement = statement
        self.do_pipeline = do_pipeline
        self.otherwise_pipeline = otherwise_pipeline

    def call_by_condition(self, data):
        statement_data = validate_handler_data(self.statement, data)
        if self.statement(**statement_data):
            self.do_pipeline.add_job(data)
        else:
            self.otherwise_pipeline.add_job(data)

    def key(self):
        return "condition_pipeline:%s:%s" % (do_pipeline.key(), otherwise_pipeline.key())


class DataFrame:
    """
    Pipeline component which represents batch data which will be forward to
    pipeline from streaming services. You can apply different functions on
    this data and build pipeline.

    Each DataFrame represents future collections.Mapping object, for example
    dict. DataFrame it's a collections of keys:values which you can extract
    and represent as a DataPoint.

    This class used as user interface for building pipeline.

    Each component which you subscribe to DataFrame has a name. Stairs
    unique_id_session will care about it's uniqueness.
    """

    def __init__(self, data_pipeline: DataPipeline, transformation: dict = None):
        """
        :param data_pipeline: DataPipeline object witch has information about
        data pipeline graph and all components

        :param transformation: Transformations which should be applied on data
        when it comes from streaming service
        """
        self.data_pipeline = data_pipeline
        self.transformation = transformation or {}

    def rename(self, **transformations) -> 'DataFrame':
        """
        Rename some DataFrame item. DataFrame it's like dict object, and you
        can easily rename some of it's key on a different one.

        :param transformations: transformation map from one key to another
        :return:
        """

        for key, value in transformations.items():
            if isinstance(value, DataPoint):
                transformations[key] = value.get_key()

        # reverse value to key, because KeyToKey search for values here
        transformation = {v: k for k, v in transformations.items()}
        transformation = self.update_by_current_transformation(transformation)

        return self.__class__(self.data_pipeline, transformation)

    def get(self, key: str) -> 'DataPoint':
        """
        Extract one key from DataFrame. It will converted to DataPoint object
        which will have similar behavior as a DataFrame but it's just one
        item (like dict with one item inside {"item": "value"}).

        :param key: Key of item which you want to extract from DataFrame
        :return: DataPoint
        """
        return DataPoint(self.data_pipeline, {key: key})

    def subscribe_flow(self, flow: Flow, as_worker=False, name=None) \
            -> 'DataFrame':
        """
        Subscribe stairs.Flow component to data.

        Subscribe function merge result of some Flow component and current data
        which stored in DataFrame. For example if DataFrame represents following
        data:
            {'a': 1, 'b': 2}
        And Flow component will return:
            {'b': 3, 'c': 4}
        As a result you will have DataFrame which represents following dict
        object:
            {'a': 1, 'b': 3, 'c': 4}

        subscribe - it's a way to accumulate some data during pipeline execution

        :param flow: stairs.Flow object which return `Mapping` like data

        :param as_worker: If True call Flow through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for function which will process current
        DataFrame data

        :return: new DataFrame which will represents data after Flow
        subscription
        """

        data_pipeline = self.data_pipeline.deepcopy()

        # build name for Flow component, based on custom name or pipeline name
        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  flow.name())

        config = data_pipeline.worker_info.config
        # Build Pipeline component which represents stairs Flow
        p_component = PipelineFlow(self.data_pipeline,
                                   flow,
                                   as_worker=as_worker,
                                   name=name,
                                   config=config,
                                   update_pipe_data=True)

        # Add new pipeline component to common graph
        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        # Return new DataFrame built on new pipeline graph
        return DataFrame(data_pipeline)

    def subscribe_flow_as_producer(self, flow: Flow, as_worker=False,
                                   name=None) -> 'DataFrame':
        """
        Subscribe stairs.Flow as a producer component. It's similar to
        DataFrame.subscribe_flow but with producer behaviour.

        Producer component allows you to return batch of Mapping objects (dicts)
        and send them to streaming service in one transaction.

        Flow as a producer should return iterator (e.g. list) of dicts like
        objects. Stairs will iterate Flow and forward each item to streaming
        service.

        :param flow: stairs.Flow object which return `Mapping` like data

        :param as_worker: If True call Flow through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for function which will process current
        DataFrame data

        :return: new DataFrame which will represents data after Flow
        subscription
        """
        data_pipeline = self.data_pipeline.deepcopy()

        # build name for Flow component, based on custom name or pipeline name
        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  flow.name())

        config = data_pipeline.worker_info.config
        # Build Pipeline component which represents stairs Flow as a producer
        p_component = PipelineFlowProducer(self.data_pipeline,
                                           flow,
                                           as_worker=as_worker,
                                           name=name,
                                           config=config,
                                           update_pipe_data=True)

        # Add new pipeline component to common graph
        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        # Return new DataFrame built on new pipeline graph
        return DataFrame(data_pipeline)

    def subscribe_pipeline(self, app_pipeline, config: dict = None) \
            -> 'DataFrame':
        """
        Subscribe another pipeline to current DataFrame.

        (!!) Subscribe pipeline it's not very performance friendly function, it
        will duplicate current DataFrame data and forward it to the end
        of new pipeline for merging it. If you need performance use
        `call_pipeline` or `apply_pipeline`.

        Stairs will merge graph of new pipeline with current one. It will by
        one chain of components. New pipeline graph will be added to the leaves
        of the current graph.

        Subscribe function merge result of pipeline with current
        data which stored in DataFrame.


                (2) -> (new pipeline 1) -> (new pipeline 2) -> merge data
        (1) ->                                                  /
                - - - - - - - - -  - - - - - - - - - - - - - - /
                         data duplication from (1)

        Subscribe pipeline basically means that last step will merge data with
        current DataFrame.

        :param app_pipeline: stairs.App.Pipeline object

        :param config: dict like object with config for another pipeline

        :return: new DataFrame which will represents data after pipeline
        subscription
        """

        data_pipeline = self.data_pipeline.deepcopy()

        # Make completely new pipeline based on different config
        pipeline = app_pipeline.make_pipeline(config=config)

        # duplicating the root, and prepare to be worker
        pipeline.graph.get_root().reinit_component()
        # set the root of pipeline as a worker
        pipeline.graph.get_root().p_component.as_worker = True

        # Step where all data will be merge into one
        pipeline.graph.add_pipeline_component(
            PipelineConnectorComponent(pipeline))

        # merge new pipeline with a current one
        data_pipeline.add_pipeline_graph(pipeline.graph,
                                         self.transformation,
                                         forward_data_to_leaves=True)

        # Return new DataFrame built on new pipeline graph
        return DataFrame(data_pipeline)

    def call_pipeline(self, pipeline, when=None) -> 'DataFrame':
        """
        Call kind of commands allows you to forward current DataFrame data
        to new pipeline and ignore any results from this pipeline.

        It's similar to pipeline.add_job command but will be executed
        during pipeline execution.

        If current DataFrame represents following data:
            {'a': 1, 'b': 2}
        Result of new DataFrame will be completely the same no matter what
        new pipeline will return:
             {'a': 1, 'b': 2}

        New pipeline will be executed in a background and will not related
        to current one.

        :param pipeline: App.pipeline instance
        :return: Return new DataFrame built on new pipeline graph
        """

        def forward_data_to_pipeline(**kwargs):
            if isinstance(pipeline, ConditionPipeline):
                pipeline.call_by_condition(kwargs)
            else:
                pipeline.add_job(kwargs)
            return dict()

        return self.subscribe_func(forward_data_to_pipeline,
                                   name='call_pipeline:%s' % str(pipeline.key()),
                                   when=when,
                                   as_worker=False)

    def apply_pipeline(self, app_pipeline, config=None) -> 'DataFrame':
        """
        Apply new pipeline with a current one.

        Apply kind of functions means that current DataFrame data will be
        overwritten by new pipeline (in case of keys duplication). Each
        step of new pipeline could overwrite your current data and as a result
        you will have data from last step of new pipeline.

        New pipeline will extend current one and all components of new pipeline
        will be added to the leaves of the current one.

        Apply functions example:
            If current DataFrame has following data:
            {'a': 1, 'b': 2}

            And new pipeline return:
            {'b': 3, 'c': 4}

            As a result you will have:
            {'a': 1, 'b': 3, 'c': 4}

            If you want to have this:
            {'b': 3, 'c': 4}
            Then you probably need to use `call_pipeline`

        :param app_pipeline: App.pipeline instance

        :param config: dict like object with config for another pipeline

        :return: Return new DataFrame built on new pipeline graph
        """
        data_pipeline = self.data_pipeline.deepcopy()

        pipeline = app_pipeline.make_pipeline(config=config)

        # duplicating the root, and prepare to be worker
        pipeline.graph.get_root().reinit_component()
        # set the root of pipeline as a worker
        pipeline.graph.get_root().p_component.as_worker = True

        # set last components of new pipeline to not update current data
        for leave in pipeline_graph.get_leaves(pipeline.graph):
            leave.reinit_component()
            leave.p_component.update_pipe_data = True

        data_pipeline.add_pipeline_graph(pipeline.graph,
                                         self.transformation)

        return DataFrame(data_pipeline)

    def subscribe_consumer(self, consumer: Union[Consumer, ConsumerIter,
                                               StandAloneConsumer],
                           name=None, as_worker=False, when=None) -> 'DataFrame':
        """
        Subscribes consumer component.

        Consumer component has no influence on current DataFrame data, and
        will be executed in a background.

        Consumer results will be completely ignored.

        :param consumer: Consumer, ConsumerIter or StandAlone consumers
        components

        :param name: Custom name for function which will process current
        DataFrame data

        :param as_worker: If True call Consumer using streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :return: Return new DataFrame with Consumer component
        """
        data_pipeline = self.data_pipeline.deepcopy()

        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  consumer.name())

        p_component = PipelineOutput(
            self.data_pipeline,
            name=name,
            component=consumer,
            config=data_pipeline.worker_info.config,
            as_worker=as_worker,
            when_handler=when,
            update_pipe_data=True
        )

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def subscribe_func(self, func, as_worker=False, name=None,
                       when=None, key_wrapper=None) -> 'DataFrame':
        """
        Subscribes any user functions or methods.

        If function is a lambda function you should always specify custom name,
        otherwise there is a chance for data collisions.

        Subscribe function merge result from a new function with current
        DataFrame data. For example if DataFrame represents following
        data:
            {'a': 1, 'b': 2}
        And function component will return:
            {'b': 3, 'c': 4}
        As a result you will have DataFrame which represents following dict
        object:
            {'a': 1, 'b': 3, 'c': 4}

        :param func: Any functions you want

        :param as_worker: If True call function using streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for a function

        :param update_pipe_data:

        :return: Return new DataFrame with function component
        """
        data_pipeline = self.data_pipeline.deepcopy()

        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  func.__name__)

        config = data_pipeline.worker_info.config
        p_component = PipelineFunction(self.data_pipeline,
                                       func,
                                       as_worker=as_worker,
                                       name=name,
                                       config=config,
                                       when_handler=when,
                                       key_wrapper=key_wrapper,
                                       update_pipe_data=True)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def subscribe_func_as_producer(self, func, as_worker=False, name=None,
                                   _update_pipe_data=True,
                                   when=None) -> 'DataFrame':
        """
        Subscribes function as a producer component. It's similar to
        DataFrame.subscribe_func but with producer behaviour.

        Producer component allows you to return batch of Mapping objects (dicts)
        and send them to streaming service in one transaction.

        Function as a producer should return iterator (e.g. list) of dicts like
        objects. Stairs will iterate by this function and forward each item
        to streaming service.

        :param func: Function which return batch of `Mapping` like data

        :param as_worker: If True call function through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for a function which will process current
        DataFrame data

        :return: Return new DataFrame with function component
        """

        data_pipeline = self.data_pipeline.deepcopy()
        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  func.__name__)

        config = data_pipeline.worker_info.config
        p_component = PipelineFunctionProducer(self.data_pipeline,
                                               func,
                                               as_worker=as_worker,
                                               name=name,
                                               config=config,
                                               when_handler=when,
                                               update_pipe_data=_update_pipe_data)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )
        return DataFrame(data_pipeline)

    def apply_func(self, func, as_worker=False, name=None) -> 'DataFrame':
        """
        Apply function for future data in current DataFrame.

        Apply function completely replace current data with result of this
        function. For example:
            If you have following data in current DataFrame:
            {'a': 1, 'b': 2}
            And your function return:
            {'c': 3}
            As a result you will have:
            {'c': 3}

        :param func: Function which return batch of `Mapping` like data

        :param as_worker: If True call function through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for a function which will process current
        DataFrame data

        :return: Return new DataFrame with function component
        """
        data_pipeline = self.data_pipeline.deepcopy()

        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  func.__name__)

        config = data_pipeline.worker_info.config
        p_component = PipelineFunction(self.data_pipeline,
                                       func,
                                       as_worker=as_worker,
                                       name=name,
                                       config=config,
                                       update_pipe_data=False)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def apply_func_as_producer(self, func, as_worker=False, name=None)\
            -> 'DataFrame':
        """
        Similar to subscribe_func_as_producer,  but in this case result
        of current data will be completely replace by new function (similar to
        apply_func)

        :param func: Function which return batch of `Mapping` like data

        :param as_worker: If True call function through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for a function which will process current
        DataFrame data

        :return: Return new DataFrame with function component
        """
        return self.subscribe_func_as_producer(func,
                                               as_worker=as_worker,
                                               name=name,
                                               _update_pipe_data=False)

    def apply_flow(self, flow, name=None, as_worker=False,) -> 'DataFrame':
        """
        Similar to subscribe_flow but will completely replace current DataFrame
        data with result of new Flow. (similar e.g. to apply_func)

        :param flow: stairs.Flow object which return `Mapping` like data

        :param as_worker: If True call Flow through streaming service. (Put
        input data to streaming service queue, and then read in a different
        process)

        :param name: Custom name for function which will process current
        DataFrame data

        :return: new DataFrame which will represents data after Flow
        subscription
        """
        data_pipeline = self.data_pipeline.deepcopy()

        # build name for Flow component, based on custom name or pipeline name
        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  flow.name())

        config = data_pipeline.worker_info.config
        # Build Pipeline component which represents stairs Flow
        p_component = PipelineFlow(self.data_pipeline,
                                   flow,
                                   as_worker=as_worker,
                                   name=name,
                                   config=config,
                                   update_pipe_data=False)

        # Add new pipeline component to common graph
        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        # Return new DataFrame built on new pipeline graph
        return DataFrame(data_pipeline)

    def add_value(self, **kwargs) -> 'DataFrame':
        """
        Add constant value to current DataFrame:

        :param kwargs: dict like object with keys:values which you want to
        add to current DataFrame

        :return: Return new DataFrame with new custom data
        """
        func = lambda **k: kwargs

        # it's safe to use uuid4 as name, because it can't be a worker
        return self.subscribe_func(func,
                                   name=uuid.uuid4(),
                                   as_worker=False)

    def update_by_current_transformation(self, new_transformation) -> dict:
        for key, value in self.transformation.items():
            if key == value:
                continue

            if value in new_transformation:
                new_transformation[key] = new_transformation[value]
                del new_transformation[value]
            else:
                new_transformation[key] = value

        return new_transformation

    def has_one_transformation(self):
        return len(self.transformation) == 1


class DataPoint(DataFrame):
    """
    Object which represents DataFrame with only one key.
    """
    def get_key(self):
        return list(self.transformation.keys())[0]


def compile_pipeline(pipeline):
    """
    Go through all graph nodes, and retranslate them to stepist graph.
    Should be executed once for one pipeline (pipeline component). Otherwise
    Stepist will raise Error about handler duplication


    :param pipeline: pipeline with graph object.
    :return: dict (map) stepist_unique_id: stepist.step
    """

    stepist_app = pipeline.app.project.stepist_app
    stepist_steps = dict()
    dfs_iteration = pipeline_graph.dfs(pipeline.graph.get_root(), backward=True)

    for i, graph_item in enumerate(dfs_iteration):
        unique_id = pipeline.get_stepist_id(graph_item.p_component.id)
        graph_item.p_component.stepist_id = unique_id

        if len(graph_item.next) == 0:
            step = stepist_app.step(
                None,
                as_worker=graph_item.p_component.as_worker,
                unique_id=unique_id,
                name=unique_id,
            )(graph_item.p_component)

        elif len(graph_item.next) == 1:
            next = graph_item.next[0]
            next_stepist = stepist_steps.get(
                pipeline.get_stepist_id(next.p_component.id)
            )

            step = stepist_app.step(
                next_stepist,
                as_worker=graph_item.p_component.as_worker,
                unique_id=unique_id,
                name=unique_id,
            )(graph_item.p_component)

        else:
            raise RuntimeError("next more than one, implement map support")

        stepist_steps[unique_id] = step

    return stepist_steps


def condition_pipeline(statement, do_pipeline, otherwise_pipeline=None):
    return ConditionPipeline(statement, do_pipeline, otherwise_pipeline)


def concatenate(*data_frames: DataFrame,
                **data_points_or_frames: Union[DataFrame, DataPoint]) \
        -> DataFrame:
    """
    Concatenate DataFrame's objects or DataPoint's into one DataFrame.

    Example:

    if one DataFrame represents as:
        df1 -> {'a': 1, 'b': 2}
    another as:
        df2 -> {'c': 3, 'd': 4}
    you can concatenate simple keys into one DataFrame:
        df3 = concatenate(b=df1.get('b'), c=df2.get('c'))

    It's not performance friendly operation for long DataFrames (DataFrames
    which has a lot of function). Because data will be duplicated and merged
    in the end of first pipeline graph.

    :param data_frames: list of DataFrames to concatenate together, if
    some keys overlapped - it will replaced by latest DataFrame.

    :param data_points_or_frames: mapping of key->DataPoint or key->DataFrame
    which will store in new DataFrame. If value is a DataFrame it will be wrapped
    in new key.

    :return: New DataFrame which represents concatenation of provided values.
    """
    ensure_concatenate_allowed(data_frames, data_points_or_frames)

    base_pipeline = None
    if data_frames:
        base_pipeline = data_frames[0].data_pipeline
    if data_points_or_frames:
        base_pipeline = list(data_points_or_frames.values())[0].data_pipeline

    keys = sorted(data_points_or_frames.keys())
    connector_name = "%s:%s" % ("concatenate", "/".join(keys))
    last_p_component = PipelineConnectorComponent(
        pipeline=base_pipeline,
        name=connector_name)

    # TODO: check if all data_pipelines_transformations actually transformations

    data_pipeline_items = [dp.data_pipeline for dp in data_points_or_frames.values()]

    # generate's result transformation object
    for key, data_point_or_frame in data_points_or_frames.items():

        p = data_point_or_frame.data_pipeline
        leaves = pipeline_graph.get_leaves(p.graph)

        assert len(leaves) == 1
        if isinstance(data_point_or_frame, DataPoint):
            data_point = data_point_or_frame
            keys_to_transform = {data_point.get_key(): key}
            transformation_func = transformations_types.KeyToKey(keys_to_transform)
            leaves[0].p_component.add_context(last_p_component,
                                              transformation_func)
        else:
            data_frame = data_point_or_frame
            keys_to_transform = data_frame.transformation
            transformation_func = transformations_types.KeysToDict(key,
                                                            keys_to_transform)
            leaves[0].p_component.add_context(last_p_component,
                                              transformation_func)

    for data_frame in data_frames:
        p = data_frame.data_pipeline
        leaves = pipeline_graph.get_leaves(p.graph)
        assert len(leaves) == 1
        keys_to_transform = data_frame.transformation
        transformation_func = transformations_types.AllKeys(keys_to_transform)
        leaves[0].p_component.add_context(last_p_component,
                                          transformation_func)

    base_data_pipeline = copy.copy(data_pipeline_items[0])
    base_graph = base_data_pipeline.graph
    base_worker_info = data_pipeline_items[0].worker_info

    for data_pipeline in data_pipeline_items[1:]:

        if data_pipeline.worker_info is not base_worker_info:
            raise RuntimeError("Not possible to use different worker configs, "
                               "for pipeline")

        graph, base_g_item, handler_item = \
            concatenate_sequentially(base_graph, data_pipeline.graph)

        base_graph = graph

    base_graph.add_pipeline_component(last_p_component)

    return DataFrame(base_data_pipeline)


def ensure_subscribe_allowed(transformation):
    for key, data_pipeline in transformation.items():
        graph_leaves = pipeline_graph.get_leaves(data_pipeline.graph)

        if not graph_leaves[0].p_component.get_one_value:
            raise RuntimeError("Found multiple values for "
                               "one transformation key")


def ensure_concatenate_allowed(data_frames, data_points_or_frames):

    for frame in data_frames:
        if not isinstance(frame, DataFrame):
            raise RuntimeError("%s is not a DataFrame object" % frame)

    for key, point in data_points_or_frames.items():
        if not isinstance(point, DataPoint) and not isinstance(point, DataFrame):
            raise RuntimeError("DataPoint with name:%s is not a DataPoint" % key)
