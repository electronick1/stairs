import inspect

from stairs.core.utils import names
from stairs.core.session import config_session


def config():
    def _wrap_config(handler):
        handler_name = names.func_module_name(handler)
        config_session.register_config(handler_name, handler)

        args, _, _, values = inspect.getargvalues(handler)

        args_result = []

        def _wrap_handler(**kwargs):
            for dep_config_handler in args:
                args_result.append(dep_config_handler(**kwargs))

            return handler(*args_result, **kwargs)

        return _wrap_handler

    return _wrap_config
