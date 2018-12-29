
import inspect
import collections

from stairs.core.utils.attrdict import AttrDict

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

    def __reconnect__(self):
        pass

    def compile(self, pipeline):
        self.pipeline = pipeline

        __flow_steps = dict()

        self.__reconnect__()

        for flow_step_handler_name in self.__ordered__:
            flow_step_handler = getattr(self, flow_step_handler_name, None)

            if not flow_step_handler:
                continue

            handler_meta = getattr(flow_step_handler, '__meta__', None)
            if not handler_meta:
                continue

            flow_step = handler_meta.get('step', None)
            if not flow_step:
                continue

            next_steps = []

            if flow_step.next_steps:
                for next_step in flow_step.next_steps:
                    if next_step is None:
                        next_steps.append(None)
                    else:
                        next_steps.append(__flow_steps[next_step.__name__])

            step = StairsStep(pipeline,
                              self,
                              flow_step.handler,
                              next_steps,
                              save_result=flow_step.save_result,
                              name=flow_step.name)
            __flow_steps[flow_step.handler.__name__] = step
            setattr(self, flow_step.handler.__name__, step)

        self.__steps = __flow_steps

    def start_from(self, step, **data):
        return step(**data)

    @classmethod
    def __name__(cls):
        module_name = inspect.getmodule(cls).__name__
        return "%s:%s" % (cls.__name__, module_name)

    @classmethod
    def key(cls):
        return str(cls.__name__)

    @classmethod
    def name(cls):
        return cls.__name__


def get_fields(flow):
    return flow.__dict__.get('__annotations__', {})
