from . import local


def storage():
    if not hasattr(local, 'unique_ids'):
        local.unique_ids = dict(
            names=dict()
        )
    return local.unique_ids


def reserve_id_by_name(name):
    if name in storage()['names']:
        id = storage()['names'][name] + 1
    else:
        id = 0

    set_current_id_by_name(name, id)
    return id


def set_current_id_by_name(name, id):
    storage()['names'][name] = id
