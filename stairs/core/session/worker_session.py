from . import local
from contextlib import contextmanager


def storage():
    if not hasattr(local, 'worker_storage'):
        local.worker_storage = dict()

    return local.worker_storage


def set_worker(worker):
    storage()['current_worker'] = worker


def get_worker():
    return storage().get('current_worker', None)


@contextmanager
def change_worker_ctx(worker):
    prev_worker = get_worker()

    try:
        set_worker(worker)
        yield
    finally:
        set_worker(prev_worker)
