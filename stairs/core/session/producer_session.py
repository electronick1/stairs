from contextlib import contextmanager

from . import global_stairs


def storage():
    if not hasattr(global_stairs, 'producer'):
        global_stairs.producer = dict(
            custom_callbacks=None,
        )

    return global_stairs.producer


def set_custom_callbacks(custom_callbacks):
    storage()['custom_callbacks'] = custom_callbacks


def get_custom_callbacks():
    return storage()['custom_callbacks']


@contextmanager
def change_custom_callbacks(custom_callbacks):
    prev_custom_callbacks = get_custom_callbacks()
    try:
        set_custom_callbacks(custom_callbacks)
        yield
    finally:
        set_custom_callbacks(prev_custom_callbacks)
