import copy
import types

from collections import Iterable, Mapping

from stepist.flow.utils import validate_handler_data, StopFlowFlag

from stairs.core.utils.execeptions import StopPipelineFlag

from stairs.core.pipeline.pipeline_objects import context as pipeline_context
from stairs.core.session import unique_id_session


class PipelineComponent:

    def __init__(self, pipeline, component, name, config,
                 as_worker=False, id=None, update_pipe_data=False):

        self.component = component
        self.pipeline = pipeline

        self.config = config

        self.update_pipe_data = update_pipe_data

        self._context_list = []

        self.as_worker = as_worker

        self.pre_id = id or name
        self.id = self.gen_unique_id()
        self.name = name
        self.stepist_id = None

    def run_component(self, data):
        data = validate_handler_data(self.component, data)
        try:
            return self.component(**data)
        except StopPipelineFlag as e:
            raise StopFlowFlag(e)

    def add_context(self, p_component, transformation):
        self._context_list.append(
            pipeline_context.ComponentContext(p_component, transformation)
        )

    def gen_unique_id(self):
        return "%s:%s" % (
            str(self.pre_id),
            unique_id_session.reserve_id_by_name(self.pre_id)
        )

    def copy_component(self):
        new_component = copy.copy(self)
        # regenerate unique id
        new_component.id = new_component.gen_unique_id()

        return new_component

    def validate_output_data(self, data):
        output_data = dict()
        data_to_transform = dict()

        # remove all data related to current component
        for key, value in data.items():
            # skip values if it's not "subscribe" statement
            related_to_current_component = pipeline_context\
                .belongs_to_component(self, key)

            # skip data which was address to current component but it was
            # "apply" type component
            if related_to_current_component and not self.update_pipe_data:
                continue

            if not pipeline_context.is_context_key(key):
                if not self._context_list:
                    output_data[key] = value

                # collect data for transformation
                data_to_transform[key] = value
            else:
                # in case if data related to current component
                # but it should update global pipeline data, let's reassign
                # it to next component
                if related_to_current_component and self.update_pipe_data:
                    unassign_key = pipeline_context.unassign_data(self, key)
                    data_to_transform[unassign_key] = value
                else:
                    output_data[key] = value

        # in case if component has context list (has next components)
        # let's reassign data
        if self._context_list:
            # apply transformation and assign context labels
            for context in self._context_list:
                data = context.transformation(data_to_transform)
                output_data.update(context.assign_labels(data))
        else:
            # in case if current component is last one
            # and self.udpate_pipe_data, let's keep data
            if self.update_pipe_data:
                output_data.update(data_to_transform)

        return output_data

    def validate_input_data(self, data):
        input_data = dict()
        for key, value in data.items():
            if pipeline_context.belongs_to_component(self, key):
                new_key = pipeline_context.unassign_data(self, key)
                input_data[new_key] = value

        return input_data

    def ensure_component_result_is_valid(self, data):
        if not isinstance(data, Mapping):
            raise RuntimeError("Result of your function should be `Mapping`"
                               " like object. Result of `%s` is %s type " %
                               (self.name, type(data)))


class PipelineFlow(PipelineComponent):

    def __init__(self, pipeline, component, **kwargs):
        component.compile(pipeline)
        PipelineComponent.__init__(self,
                                   pipeline=pipeline,
                                   component=component,
                                   **kwargs)

    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)

        result = self.run_component(component_data)
        self.ensure_component_result_is_valid(result)

        # It's important to run kwargs validation and result validation
        # separately, otherwise result will be overlap by kwargs
        output_kwargs = self.validate_output_data(kwargs)
        output_result = self.validate_output_data(result)
        output = {**output_kwargs, **output_result}

        return output


class PipelineFlowProducer(PipelineComponent):
    def __init__(self, pipeline, component, **kwargs):
        component.compile(pipeline)
        PipelineComponent.__init__(self,
                                   pipeline=pipeline,
                                   component=component,
                                   **kwargs)

    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)

        result = self.run_component(component_data)

        if isinstance(result, types.GeneratorType) or isinstance(result, Iterable):
            for row_data in result:
                self.ensure_component_result_is_valid(row_data)
                # It's important to run kwargs validation and result validation
                # separately, otherwise result will be overlap by kwargs
                output_kwargs = self.validate_output_data(kwargs)
                output_row_data = self.validate_output_data(row_data)
                output = {**output_kwargs, **output_row_data}

                yield output
        else:
            raise RuntimeError("Flow producer should be a generator")


class PipelineFunction(PipelineComponent):
    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)

        result = self.run_component(component_data)
        self.ensure_component_result_is_valid(result)

        output_kwargs = self.validate_output_data(kwargs)
        output_result = self.validate_output_data(result)

        return {**output_kwargs, **output_result}


class PipelineFunctionProducer(PipelineComponent):
    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)

        result = self.run_component(component_data)

        if isinstance(result, types.GeneratorType) or isinstance(result, Iterable):
            for row_data in result:
                self.ensure_component_result_is_valid(row_data)
                # It's important to run kwargs validation and result validation
                # separately, otherwise result will be overlap by kwargs
                output_kwargs = self.validate_output_data(kwargs)
                output_row_data = self.validate_output_data(row_data)
                output = {**output_kwargs, **output_row_data}

                yield output
        else:
            raise RuntimeError("Function producer should be a generator")


class PipelineOutput(PipelineComponent):
    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)

        self.run_component(component_data)

        return self.validate_output_data(kwargs)


class PipelineConnectorComponent(PipelineComponent):
    def __init__(self, pipeline, name="Connector"):
        PipelineComponent.__init__(self,
                                   pipeline=pipeline,
                                   component=None,
                                   config=None,
                                   as_worker=False,
                                   name=name)

    def __call__(self, **data):
        data.update(self.validate_input_data(data))
        data = self.validate_output_data(data)

        return data
