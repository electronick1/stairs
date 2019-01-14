import uuid
import types

import stepist

from stepist.flow.steps.next_step import call_next_step
from stepist.flow.utils import validate_handler_data, StopFlowFlag

from stairs.core.utils.execeptions import StopPipelineFlag

from stairs.core.worker.pipeline_objects import context as pipeline_context


class PipelineComponent:

    def __init__(self, pipeline, component, id, config,
                 as_worker=False, name=None, update_pipe_data=False):

        self.component = component
        self.pipeline = pipeline

        self.id = str(id)
        self.config = config

        self.update_pipe_data = update_pipe_data

        self._context_list = []

        self.as_worker = as_worker
        self.name = name

    def __name__(self):
        return self.key()

    def key(self):
        return self.component.key() if self.component else "NotDefined"

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

        # apply transformation and assign context labels
        for context in self._context_list:
             data = context.transformation(data_to_transform)
             output_data.update(context.assign_labels(data))

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

        # if self.update_pipe_data:
        #     result_data = dict(**kwargs, **result)
        # else:
        #     result_data = result

        output = self.validate_output_data(dict(**kwargs, **result))

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

        if isinstance(result, types.GeneratorType):
            for row_data in result:
                if self.update_pipe_data:
                    result_data = dict(**kwargs, **row_data)
                else:
                    result_data = row_data

                yield self.validate_output_data(result_data)
        else:
            raise RuntimeError("Flow producer should be a generator")


class PipelineFunction(PipelineComponent):
    def __call__(self, **kwargs):
        component_data = self.validate_input_data(kwargs)
        flow_data = validate_handler_data(self.component, component_data)
        result = self.component(**flow_data)

        if self.update_pipe_data:
            result_data = dict(**kwargs, **result)
        else:
            result_data = result

        return self.validate_output_data(result_data)


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
                                   id=uuid.uuid4(),
                                   config=None,
                                   as_worker=False,
                                   name=name)

    def __call__(self, **data):
        data.update(self.validate_input_data(data))
        data = self.validate_output_data(data)
        return data
