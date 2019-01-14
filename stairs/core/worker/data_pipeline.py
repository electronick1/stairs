import copy

from stepist.flow.steps.next_step import call_next_step as stepist_next_step
import stepist

from stairs.core.worker.pipeline_objects import (PipelineFlow,
                                                 PipelineFlowProducer,
                                                 PipelineOutput,
                                                 PipelineFunction,
                                                 PipelineInVainComponent)

from stairs.core.worker import pipeline_graph
from stairs.core.worker.pipeline_graph import concatenate_sequentially

from stairs.core.worker.pipeline_objects import \
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
                initial_component = PipelineInVainComponent(pipeline=self)

            graph.add_pipeline_component(initial_component)

        self.graph = graph
        self.stepist_steps = dict()

    def __call__(self, *args, **kwargs):
        if not self.stepist_steps:
            raise RuntimeError("Pipeline not compiled")

        stepist_next_step(self.get_stepist_root(), **kwargs)

    def __copy__(self):
        cls = self.__class__

        return cls(self.app,
                   worker_info=self.worker_info,
                   graph=copy.copy(self.graph))

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

    def add_pipeline_graph(self, graph, transformations):
        graph_items_affected = self.graph.add_graph_on_leaves(graph)

        root_p_component = graph.get_root().p_component

        for g_item in graph_items_affected:
            g_item.p_component.add_context(root_p_component, transformations)

    def get_last_item(self):
        return pipeline_graph.get_leaves(self.graph)[0]

    def compile(self):
        if self.is_compiled():
            raise RuntimeError("Pipeline already compiled")

        self.stepist_steps = compile_pipeline(self)

    def is_compiled(self):
        return bool(self.stepist_steps)

    # utils:

    def stepist_component_id(self, component, index=0):
        return "%s:%s:%s" % (self.worker_info.key(),
                             component.id,
                             index)

    def get_stepist_root(self):
        root_component = self.graph.get_root().p_component
        stepist_id = self.stepist_component_id(root_component)

        return self.stepist_steps[stepist_id]

    def get_unique_id(self, name):
        # TODO: make proper name generator
        name = str(name)

        # preffix = ''
        # for i in range(self.flows_count() + 1):
        #     if name + preffix not in self.flows:
        #         return name + preffix
        #
        #     preffix = '_%s' % i

        import uuid
        if not name:
            name = uuid.uuid4()

        return self.get_worker_id(name)

    def get_worker_id(self, pipeline_object_id):
        return "%s:%s" % (self.worker_info.key(), pipeline_object_id)

    @classmethod
    def make_empty(cls, app, worker_info):
        return cls(
            app,
            worker_info,
            initial_component=PipelineInVainComponent(pipeline=None,
                                                      name="root")
        )


class DataFrame:

    def __init__(self, data_pipeline: DataPipeline, transformation: dict=None,
                 possible_keys=None):

        self.data_pipeline = data_pipeline
        self.transformation = transformation or {}
        self.possible_keys = possible_keys or []

    def make(self, *keys, **transformation):
        # populate transformation by keys
        for key in keys:
            transformation[key] = key

        for key, value in transformation.items():
            if isinstance(value, DataPoint):
                transformation[key] = value.get_key()

        transformation = {v: k for k, v in transformation.items()}
        transformation = self.update_by_current_transformation(transformation)

        return self.__class__(self.data_pipeline, transformation)

    def get(self, key):
        return DataPoint(self.data_pipeline, {key: key})

    def update_by_current_transformation(self, new_transformation) -> dict:
        for key, value in self.transformation:
            if value in new_transformation:
                new_transformation[key] = new_transformation[value]
                del new_transformation[value]
            else:
                new_transformation[key] = value

        return new_transformation

    def has_one_transformation(self):
        return len(self.transformation) == 1

    def subscribe_flow(self, flow, as_worker=False, update_pipe_data=True,
                       name=None):
        data_pipeline = copy.copy(self.data_pipeline)

        name = name or "%s:%s" % (self.data_pipeline.worker_info.key(),
                                  flow.name())

        id = name or flow.key()
        config = data_pipeline.worker_info.config

        p_component = PipelineFlow(self.data_pipeline,
                                   flow,
                                   as_worker=as_worker,
                                   id=id,
                                   name=name,
                                   config=config,
                                   update_pipe_data=update_pipe_data)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def subscribe_flow_as_generator(self, flow, as_worker=False,
                                    update_pipe_data=True):
        data_pipeline = copy.copy(self.data_pipeline)

        id = data_pipeline.get_unique_id(flow.key())
        config = data_pipeline.worker_info.config

        p_component = PipelineFlowProducer(self.data_pipeline,
                                           flow,
                                           as_worker=as_worker,
                                           id=id,
                                           name=flow.key(),
                                           config=config,
                                           update_pipe_data=update_pipe_data)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def subscribe_pipeline(self, app_worker, name=None, config=None,
                           update_pipe_data=True):
        data_pipeline = copy.copy(self.data_pipeline)
        id = data_pipeline.get_unique_id(name or 'worker')

        pipeline = app_worker.make_pipeline(config=config)
        pipeline.graph.root.p_component.as_worker = True

        self.data_pipeline.add_pipeline_graph(pipeline.graph,
                                              self.transformation)

        return DataFrame(data_pipeline)

    def subscribe_consumer(self, output, name=None, as_worker=False):
        data_pipeline = copy.copy(self.data_pipeline)

        name = name or "%s:%s" % ("output", output.name())

        p_component = PipelineOutput(
            self.data_pipeline,
            name=name,
            component=output,
            id=name,
            config=data_pipeline.worker_info.config,
            as_worker=as_worker,
            update_pipe_data=False
        )

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def subscribe_func(self, func, as_worker=False, name=None,
                       update_pipe_data=True):
        data_pipeline = copy.copy(self.data_pipeline)

        id = data_pipeline.get_unique_id(func.__name__)
        config = data_pipeline.worker_info.config

        p_component = PipelineFunction(self.data_pipeline,
                                       func,
                                       as_worker=as_worker,
                                       id=id,
                                       name=name,
                                       config=config)

        data_pipeline.add_pipeline_component(
            p_component,
            transformation=self.transformation
        )

        return DataFrame(data_pipeline)

    def apply_flow(self, flow, as_worker=False,):
        return self.subscribe_flow(flow=flow,
                                   as_worker=as_worker,
                                   update_pipe_data=False)

    def apply_pipeline(self, app_worker, name=None, config=None,
                    update_pipe_data=True):

        return self.subscribe_pipeline(app_worker, name=name, config=config,
                                       update_pipe_data=update_pipe_data)


class DataPoint(DataFrame):
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

        unique_id = pipeline.stepist_component_id(graph_item.p_component)

        if len(graph_item.next) == 0:
            step = stepist_app.step(
                None,
                as_worker=graph_item.p_component.as_worker,
                unique_id=unique_id
            )(graph_item.p_component)

        elif len(graph_item.next) == 1:
            next = graph_item.next[0]
            next_stepist_id = pipeline.stepist_component_id(next.p_component)
            next_stepist = stepist_steps.get(next_stepist_id)

            step = stepist_app.step(
                next_stepist,
                as_worker=graph_item.p_component.as_worker,
                unique_id=graph_item.p_component.id
            )(graph_item.p_component)

        else:
            raise RuntimeError("next more than one, implement map support")

        stepist_steps[unique_id] = step

    return stepist_steps


def concatenate(*data_frames, **data_points):
    """

    """
    #ensure_concatenate_allowed(data_points)

    base_pipeline = None
    if data_frames:
        base_pipeline = data_frames[0].data_pipeline
    if data_points:
        base_pipeline = list(data_points.values())[0].data_pipeline

    last_p_component = PipelineInVainComponent(
        pipeline=base_pipeline,
        name="%s:%s" % ("concatenate", ",".join(data_points.keys())))

    # TODO: check if all data_pipelines_transformations actually transformations

    data_pipeline_items = [dp.data_pipeline for dp in data_points.values()]

    # generate's result transformation object
    for key, data_point in data_points.items():

        p = data_point.data_pipeline
        leaves = pipeline_graph.get_leaves(p.graph)

        assert len(leaves) == 1
        if isinstance(data_point, DataPoint):
            keys_to_transform = {data_point.get_key(): key}
            transformation_func = transformations_types.KeyToKey(keys_to_transform)
            leaves[0].p_component.add_context(last_p_component,
                                              transformation_func)
        else:
            keys_to_transform = data_point.transformation
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


def ensure_concatenate_allowed(transformation):

    for key, data_pipeline in transformation.items():
        graph_leaves = pipeline_graph.get_leaves(data_pipeline.graph)

        if len(graph_leaves) > 1:
            raise RuntimeError("Can't concatenate multiple leaves graph")

        # if not graph_leaves[0].pipeline_component.get_one_value:
        #     raise RuntimeError("Found multiple values for "
        #                        "one transformation key")
