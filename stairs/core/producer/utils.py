import time
import logging


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
