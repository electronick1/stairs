from typing import Union
import inspect
import collections

from stairs.core.utils import AttrDict

from stairs.core.flow.step import StairsStep


class FlowMeta(type):

    @classmethod
    def __prepare__(metacls, name, bases, **kwds):
        return collections.OrderedDict()

    def __new__(self, name, bases, classdict):
        classdict['__ordered__'] = []

        for base in bases:
            classdict['__ordered__'].extend(base.__ordered__)

        cls_methods = [key for key in classdict.keys()]
        classdict['__ordered__'].extend(cls_methods)

        return type.__new__(self, name, bases, classdict)


class Flow(metaclass=FlowMeta):

    def __reconnect__(self) -> None:
        """
        Abstract function which could be define by user for change Flow
        structure
        """
        pass

    def compile(self, pipeline) -> None:
        self.pipeline = pipeline

        __flow_steps = dict()

        for flow_step_handler_name in self.__ordered__:
            recursive_initializer(self.pipeline,
                                  self,
                                  __flow_steps,
                                  flow_step_handler_name)

        for handler_name, stairs_step in __flow_steps.items():
            setattr(self, handler_name, stairs_step)

        # Call custom user function for change flow graph
        self.__reconnect__()
        self.__steps = __flow_steps

    def start_from(self, step: StairsStep, **data) -> AttrDict:
        return step(**data)

    @classmethod
    def __name__(cls):
        module_name = inspect.getmodule(cls).__name__
        return "%s:%s" % (cls.__name__, module_name)

    @classmethod
    def key(cls) -> str:
        return str(cls.__name__)

    @classmethod
    def name(cls) -> str:
        return str(cls.__name__)


def get_fields(flow):
    return flow.__dict__.get('__annotations__', {})


def recursive_initializer(pipeline,
                          flow_obj: Flow,
                          steps_data:dict,
                          func_name: str) -> Union[StairsStep, None]:
    """
    Trying to initialized in recursive way all next_steps for each
    FlowStep objects in current Flow instance.

    Return None in case of func_name not found in flow_obj
    """
    flow_step_handler = getattr(flow_obj, func_name, None)

    if not flow_step_handler:
        return None

    handler_meta = getattr(flow_step_handler, '__meta__', None)
    if not handler_meta:
        return None

    flow_step = handler_meta.get('step', None)
    if not flow_step:
        return None

    processed = steps_data.get(flow_step.handler.__name__, None)
    if processed:
        return processed

    next_steps = []

    if flow_step.next_steps:
        for next_step in flow_step.next_steps:
            if next_step is None:
                next_steps.append(None)
            else:
                next_step_object = recursive_initializer(
                    pipeline, flow_obj, steps_data, next_step.__name__)

                if next_step_object is None:
                    raise RuntimeError("%s Not found for Flow: %s" %
                                       (func_name, flow_obj.__class__.__name__))
                next_steps.append(next_step_object)

    step = StairsStep(pipeline,
                      flow_obj,
                      flow_step.handler,
                      next_steps,
                      save_result=flow_step.save_result,
                      name=flow_step.name)

    steps_data[flow_step.handler.__name__] = step

    return step