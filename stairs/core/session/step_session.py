from . import local


def storage():
    if not hasattr(local, 'step_storage'):
        local.step_storage = dict(
            steps={},
            last_registered_step=None,
        )
    return local.step_storage


def set_step(step):
    storage()['steps'][step.key()] = step


def get_step(step_key):
    return storage()['steps'].get(step_key, None)


def get_last_registered_step():
    return storage().get("last_registered_step", None)


def set_last_registered_step(step):
    storage()['last_registered_step'] = step
