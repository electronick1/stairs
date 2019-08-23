import uuid

from stepist.flow.utils import validate_handler_data
from stepist.flow.steps.hub import Hub
from stepist.flow.steps.next_step import call_next_step

from stairs.core.app_components import AppStep


def step(*next_steps, save_result=False, name=None):
    def wrapper(handler):
        s = FlowStep(handler, next_steps, save_result=save_result, name=name)
        handler.__meta__ = dict(step=s)
        return handler
    return wrapper


class StairsStepAbstract:
    # handler function which will be process data
    handler = None

    flow = None

    # stepist.step object
    step = None

    id = None

    def __name__(self):
        # All steps inside flow are not workers.
        # Let's add uuid4 for prevent instance names duplication for steps.
        return "%s:%s:%s" % (self.flow.key(),
                             self.handler.__name__,
                             self.id)

    def set_next(self, *next_steps):
        if len(next_steps) == 1:
            self.step.next_step = next_steps[0].step
        else:
            self.step.next_step = Hub(*[s.step for s in next_steps])

    def add_next(self, *next_steps):
        if self.step.next_step is not None:
            self.step.next_step = Hub(*[s.step for s in next_steps] +
                                       [self.step.next.step])
        else:
            self.set_next(*next_steps)

    def set(self, save_result=None):
        if save_result is not None:
            self.step.save_result = True

    def key(self):
        return self.__name__()

    def get_stepist_step(self):
        return self.step


class StairsStep(StairsStepAbstract, AppStep):
    step = None

    def __init__(self, pipeline, flow, handler, next_steps, save_result, name):
        self.flow = flow

        self.pipeline = pipeline
        self.handler = handler
        self.next_steps = next_steps

        self.name = name or self.handler.__name__

        self.id = uuid.uuid4()

        if len(next_steps) == 1:
            stepist_next_step = next_steps[0]
            if stepist_next_step is not None:
                stepist_next_step = stepist_next_step.step
        else:
            stepist_next_step = Hub(*[s.step for s in next_steps])

        self.step = self.pipeline\
            .app\
            .project\
            .stepist_app\
            .step(stepist_next_step,
                  unique_id=self.key(),
                  save_result=save_result,
                  name=self.name
            )(self.execute_step)

        AppStep.__init__(self, self.pipeline.app)

    def __call__(self, **data):
        flow_result = call_next_step(data, self.step)

        return flow_result

    def execute_step(self, **data):
        handler_data = validate_handler_data(self.handler, data)

        new_data = self.handler(self.flow, **handler_data)

        if new_data:
            data.update(new_data)

        if self.step.next_step is None:
            return new_data

        return data


class FlowStep:
    def __init__(self, handler, next_steps, name=None, save_result=False):
        self.handler = handler
        self.next_steps = next_steps
        self.save_result = save_result
        self.name = name


