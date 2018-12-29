from . import local

from contextlib import contextmanager


def storage():
    if not hasattr(local, 'pipeline_storage'):
        local.pipeline_storage = dict(
            pipeline=None,
        )
    return local.pipeline_storage


@contextmanager
def change_pipeline_ctx(pipeline):
    prev_pipeline = storage().get("pipeline")

    try:
        set_pipeline(pipeline)
        yield
    finally:
        set_pipeline(prev_pipeline)


def set_pipeline(pipeline):
    storage()['pipeline'] = pipeline


def get_pipeline():
    return storage().get('pipeline', None)
