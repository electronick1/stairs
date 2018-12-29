import inspect


def func_module_name(func):
    module_name = inspect.getmodule(func).__name__
    return "%s:%s" % (module_name, func.__name__)
