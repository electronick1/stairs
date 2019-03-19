import time
import logging

from stairs.core.worker.worker import Pipeline

logger = logging.getLogger(__name__)


def producer_retry(retries_amount, exceptions, sleep_time=1):

    def wrapper(func):
        def wrap_func(*args, **kwargs):

            for i in range(retries_amount):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(e)
                    time.sleep(sleep_time)

            return func(*args, **kwargs)
        return wrap_func
    return wrapper


def custom_callbacks_to_dict(custom_callbacks: list) -> dict:
    callbacks_key_value = dict()
    for c in custom_callbacks:
        name = c.get_handler_name() if isinstance(c, Pipeline) else c.__name__
        callbacks_key_value[name] = c

    return callbacks_key_value