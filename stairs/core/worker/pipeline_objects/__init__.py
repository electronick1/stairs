import types

from collections import Iterable

from stepist.flow.steps.next_step import call_next_step
from stepist.flow.utils import validate_handler_data, StopFlowFlag

from stairs.core.utils.execeptions import StopPipelineFlag

from stairs.core.worker.pipeline_objects import context as pipeline_context
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

        pre_id = id or name
        self.id = "%s:%s" % (str(pre_id),
                             unique_id_session.reserve_id_by_name(pre_id))
        self.name = name
        self.stepist_id = None

    def add_context(self, p_component, transformation):
        self._context_list.append(
            pipeline_context.ComponentContext(p_component, transformation)
        )

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


class PipelineFlow(PipelineComponent):

    def __init__(self, pipeline, component, **kwargs):
        component.compile(pipeline)
        PipelineComponent.__init__(self,
                                   pipeline=pipeline,
                                   component=component,
                                   **kwargs)

    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)
        flow_data = validate_handler_data(self.component, component_data)
        try:
            result = self.component(**flow_data)
        except StopPipelineFlag:
            raise StopFlowFlag()

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

        flow_kwargs = validate_handler_data(self.component, component_data)
        try:
            result = self.component(**flow_kwargs)
        except StopPipelineFlag:
            raise StopFlowFlag()

        if isinstance(result, types.GeneratorType) or isinstance(result, Iterable):
            for row_data in result:
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
        flow_data = validate_handler_data(self.component, component_data)
        result = self.component(**flow_data)

        output_kwargs = self.validate_output_data(kwargs)
        output_result = self.validate_output_data(result)
        return {**output_kwargs, **output_result}


class PipelineFunctionProducer(PipelineComponent):
    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)
        flow_data = validate_handler_data(self.component, component_data)
        result = self.component(**flow_data)

        if isinstance(result, types.GeneratorType) or isinstance(result, Iterable):
            for row_data in result:
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

        call_next_step(component_data, self.component.get_stepist_step())
        return self.validate_output_data(kwargs)


class PipelineInVainComponent(PipelineComponent):
    def __init__(self, pipeline, name="InVain"):
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
